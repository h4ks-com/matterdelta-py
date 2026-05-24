"""Matterbridge API interaction"""

import base64
import imaplib
import json
import tempfile
import time
from collections import OrderedDict
from pathlib import Path
from threading import Lock, Thread
from typing import Dict, List, Optional, Tuple

import requests
from deltachat2 import Bot, JsonRpcError, Message, MessageViewtype, MsgData

from .reactions import diff_reactions, reactions_by_contact

mb_config = {}
chat2gateway: Dict[Tuple[int, int], List[str]] = {}
gateway2chat: Dict[str, List[Tuple[int, int]]] = {}

# Two-way mapping between matterbridge canonical msg ids and Delta Chat msg ids
# so we can render native Delta Chat quote bubbles on inbound replies and send
# parent_id on outbound DC->MB replies (instead of relying on text embedding).
_CACHE_SIZE = 2000
_mb2dc_cache: "OrderedDict[str, Dict[Tuple[int, int], int]]" = OrderedDict()
_dc2mb_cache: "OrderedDict[Tuple[int, int, int], str]" = OrderedDict()
_cache_lock = Lock()


def _cache_put(mb_id: str, accid: int, chat_id: int, dc_msgid: int) -> None:
    if not mb_id or not dc_msgid:
        return
    with _cache_lock:
        entry = _mb2dc_cache.get(mb_id)
        if entry is None:
            entry = {}
            _mb2dc_cache[mb_id] = entry
        entry[(accid, chat_id)] = dc_msgid
        _mb2dc_cache.move_to_end(mb_id)
        while len(_mb2dc_cache) > _CACHE_SIZE:
            _mb2dc_cache.popitem(last=False)

        dc_key = (accid, chat_id, dc_msgid)
        _dc2mb_cache[dc_key] = mb_id
        _dc2mb_cache.move_to_end(dc_key)
        while len(_dc2mb_cache) > _CACHE_SIZE:
            _dc2mb_cache.popitem(last=False)


def _cache_get(mb_id: str, accid: int, chat_id: int) -> Optional[int]:
    if not mb_id:
        return None
    with _cache_lock:
        entry = _mb2dc_cache.get(mb_id)
        if entry is None:
            return None
        _mb2dc_cache.move_to_end(mb_id)
        return entry.get((accid, chat_id))


def _is_unloaded_attachment(msg: Message) -> bool:
    """True when the message has an attachment whose body hasn't landed yet."""
    return bool(msg.get("file_name") and not msg.get("file"))


# (accid, msg_id) → marker. Populated when dc2mb defers a placeholder relay
# and consumed by handle_msg_changed once delta-core fires MsgsChanged with
# the attachment body filled in.
_pending_downloads: Dict[Tuple[int, int], None] = {}
_pending_lock = Lock()


def _cache_get_mb(accid: int, chat_id: int, dc_msgid: int) -> Optional[str]:
    if not dc_msgid:
        return None
    with _cache_lock:
        key = (accid, chat_id, dc_msgid)
        mb_id = _dc2mb_cache.get(key)
        if mb_id is None:
            return None
        _dc2mb_cache.move_to_end(key)
        return mb_id


def _mb_headers() -> Optional[dict]:
    token = mb_config["api"].get("token", "")
    return {"Authorization": f"Bearer {token}"} if token else None


def _mb_post(bot: Bot, data: dict) -> dict:
    """POST a message/event to the matterbridge API; return the parsed response
    (empty dict on missing url or failure). matterbridge assigns an id we cache."""
    api_url = mb_config["api"]["url"]
    if not api_url:
        return {}
    try:
        resp = requests.post(
            api_url + "/api/message", json=data, headers=_mb_headers(), timeout=60
        )
        return resp.json() if resp.ok else {}
    except (ValueError, requests.RequestException) as ex:
        bot.logger.warning("matterbridge POST failed: %s", ex)
        return {}


# Delta Chat's SELF contact always has id 1; its reactions are the bot's own
# (set when relaying an inbound reaction) and must not loop back to the bridge.
_SELF_CONTACT_ID = 1

# Per-message snapshot of the reaction set we last forwarded, so we can diff
# against it when ReactionsChanged fires (the event says a set changed, not what).
_reactions_state: "OrderedDict[Tuple[int, int], Dict[int, set]]" = OrderedDict()
_reactions_lock = Lock()


def on_reactions_changed(bot: Bot, accid: int, event: object) -> None:
    """Diff a Delta Chat ReactionsChanged event and forward changes to the bridge."""
    chat_id = getattr(event, "chat_id", 0)
    dc_msgid = getattr(event, "msg_id", 0)
    if not dc_msgid:
        return
    mb_id = _cache_get_mb(accid, chat_id, dc_msgid)
    if not mb_id:
        return  # not a bridged message
    gateways = chat2gateway.get((accid, chat_id), [])
    if not gateways:
        return

    try:
        reactions = bot.rpc.get_message_reactions(accid, dc_msgid)
    except JsonRpcError:
        return
    new_state = reactions_by_contact(reactions)

    with _reactions_lock:
        key = (accid, dc_msgid)
        prev = _reactions_state.get(key, {})
        if new_state:
            _reactions_state[key] = new_state
            _reactions_state.move_to_end(key)
            while len(_reactions_state) > _CACHE_SIZE:
                _reactions_state.popitem(last=False)
        else:
            _reactions_state.pop(key, None)

    added, removed = diff_reactions(prev, new_state)
    for contact_id, emoji in added:
        _emit_reaction(bot, accid, contact_id, mb_id, emoji, "reaction_add", gateways)
    for contact_id, emoji in removed:
        _emit_reaction(
            bot, accid, contact_id, mb_id, emoji, "reaction_remove", gateways
        )


def _emit_reaction(
    bot: Bot,
    accid: int,
    contact_id: int,
    mb_parent: str,
    emoji: str,
    event_name: str,
    gateways: List[str],
) -> None:
    if contact_id == _SELF_CONTACT_ID:
        return  # the bot's own reaction echoed back; don't loop it
    try:
        contact = bot.rpc.get_contact(accid, contact_id)
        username = contact.display_name or contact.address or "unknown"
    except JsonRpcError:
        username = "unknown"
    for gateway in gateways:
        _mb_post(
            bot,
            {
                "username": username,
                "text": emoji,
                "emoji": emoji,
                "event": event_name,
                "parent_id": mb_parent,
                "gateway": gateway,
            },
        )


def init_api(bot: Bot, config_dir: str) -> None:
    """Load matterbridge API configuration and start listening to the API endpoint."""
    path = Path(config_dir) / "config.json"
    if path.exists():
        with path.open(encoding="utf-8") as config:
            mb_config.update(json.load(config))
    gateways = mb_config.get("gateways", [])

    for gateway in gateways:
        chat = (gateway["accountId"], gateway["chatId"])
        gateway2chat.setdefault(gateway["gateway"], []).append(chat)
        chat2gateway.setdefault(chat, []).append(gateway["gateway"])

    if mb_config["api"]["url"] and len(gateways):
        Thread(target=listen_to_matterbridge, args=(bot,), daemon=True).start()
    Thread(target=imap_cleanup_loop, args=(bot,), daemon=True).start()


def handle_msg_changed(bot: Bot, accid: int, msg_id: int) -> None:
    """Relay a previously-deferred attachment once its body has landed."""
    if not msg_id:
        return
    key = (accid, msg_id)
    with _pending_lock:
        if key not in _pending_downloads:
            return
    try:
        msg = Message(bot.rpc.get_message(accid, msg_id))
    except JsonRpcError:
        return
    if _is_unloaded_attachment(msg):
        return
    with _pending_lock:
        _pending_downloads.pop(key, None)
    dc2mb(bot, accid, msg)


def dc2mb(bot: Bot, accid: int, msg: Message) -> None:
    """Send a Delta Chat message to the matterbridge side."""
    if not msg.text and not msg.file:  # ignore buggy empty messages
        return
    # NewMessage fires before the attachment body is fetched; relaying now would
    # send the "[Image - N KiB]" placeholder. Schedule the download and defer
    # the relay until handle_msg_changed sees the body land.
    if _is_unloaded_attachment(msg):
        with _pending_lock:
            _pending_downloads[(accid, msg.id)] = None
        try:
            bot.rpc.download_full_message(accid, msg.id)
        except JsonRpcError as ex:
            bot.logger.warning(
                "download_full_message failed for msg %s: %s", msg.id, ex
            )
            with _pending_lock:
                _pending_downloads.pop((accid, msg.id), None)
        return
    gateways = chat2gateway.get((accid, msg.chat_id), [])
    if gateways:
        username = (
            msg.override_sender_name
            or bot.rpc.get_contact(accid, msg.sender.id).display_name
        )
        text = msg.text
        if text and text.split(maxsplit=1)[0] == "/me":
            event = "user_action"
            text = text[3:].strip()
        else:
            event = ""
        parent_mb_id = ""
        if msg.quote:
            quoted_dc_msgid = (
                msg.quote.get("message_id") or msg.quote.get("messageId") or 0
            )
            if quoted_dc_msgid:
                parent_mb_id = _cache_get_mb(accid, msg.chat_id, quoted_dc_msgid) or ""
        if msg.quote and not parent_mb_id and mb_config.get("quoteFormat"):
            quotenick = msg.quote.get("override_sender_name") or msg.quote.get(
                "author_display_name"
            )
            text = mb_config["quoteFormat"].format(
                MESSAGE=text,
                QUOTENICK=quotenick or "",
                QUOTEMESSAGE=" ".join(msg.quote.text.split()),
            )
        data = {"username": username, "text": text, "event": event}
        if parent_mb_id:
            data["parent_id"] = parent_mb_id
        if msg.file:
            with open(msg.file, mode="rb") as attachment:
                enc_data = base64.standard_b64encode(attachment.read()).decode()
            data["Extra"] = {
                "file": [{"Name": msg.file_name, "Data": enc_data, "Comment": text}]
            }
        for gateway in gateways:
            data["gateway"] = gateway
            bot.logger.debug("DC->MB %s", data)
            # matterbridge assigns an id on POST so other clients can
            # parent_id-reference this DC-originated message; cache it under the
            # canonical "api <id>" form matterbridge uses for parent_id resolution.
            posted_id = _mb_post(bot, data).get("id") or ""
            if posted_id:
                _cache_put("api " + posted_id, accid, msg.chat_id, msg.id)
            mb2dc(bot, data, (accid, msg.chat_id))


def _mb_reaction_to_dc(bot: Bot, msg: dict, exclude: Tuple[int, int]) -> None:
    """Apply an inbound matterbridge reaction to the bridged Delta Chat message.

    Delta Chat reacts as the bot account, so the original reactor's identity is
    lost and only one reaction per message can be held (a second cross-platform
    reaction overrides the first).
    """
    chats = [c for c in gateway2chat.get(msg["gateway"], []) if c != exclude]
    if not chats:
        return
    parent_id = msg.get("parent_id") or ""
    if parent_id in ("", "msg-parent-not-found"):
        return
    emoji = msg.get("emoji") or msg.get("text") or ""
    add = msg["event"] == "reaction_add"
    if add and not emoji:
        return
    # Vec<String> per the send_reaction JSON-RPC signature; an empty string
    # clears the bot's reaction.
    reaction = [emoji] if add else [""]
    for accid, chat_id in chats:
        dc_msgid = _cache_get(parent_id, accid, chat_id)
        if not dc_msgid:
            continue
        try:
            bot.rpc.send_reaction(accid, dc_msgid, reaction)
        except JsonRpcError as ex:
            bot.logger.warning("send_reaction failed: %s", ex)


def mb2dc(bot: Bot, msg: dict, exclude: Tuple[int, int] = (0, 0)) -> None:  # noqa: C901
    """Send a message from matterbridge to the bridged Delta Chat group"""
    if msg["event"] in ("reaction_add", "reaction_remove"):
        _mb_reaction_to_dc(bot, msg, exclude)
        return
    if msg["event"] not in ("", "user_action"):
        return
    chats = [c for c in gateway2chat.get(msg["gateway"], []) if c != exclude]
    if not chats:
        return
    text = msg.get("text") or ""
    if msg["event"] == "user_action":
        text = "/me " + text
    # matterbridge api destination has no per-message id of its own, so it
    # exposes the canonical "<protocol> <id>" via the source_id field. parent_id
    # already uses that same form, so the two are directly comparable.
    mb_id = msg.get("source_id") or msg.get("id") or ""
    parent_id = msg.get("parent_id") or ""
    # matterbridge emits this sentinel when it can't resolve the parent across the
    # bridge cache; treat it as no parent rather than searching for it.
    if parent_id == "msg-parent-not-found":
        parent_id = ""
    reply = MsgData(
        text=text,
        override_sender_name=msg["username"],
    )
    file = ((msg.get("Extra") or {}).get("file") or [{}])[0]
    if file:
        if text == file["Name"]:
            text = ""
        with tempfile.TemporaryDirectory() as tmp_dir:
            reply.file = str(Path(tmp_dir, file["Name"]))
            data = base64.decodebytes(file["Data"].encode())
            with open(reply.file, mode="wb") as attachment:
                attachment.write(data)
            if file["Name"].endswith((".tgs", ".webp")):
                reply.viewtype = MessageViewtype.STICKER
            for accid, chat_id in chats:
                reply.quoted_message_id = (
                    _cache_get(parent_id, accid, chat_id) if parent_id else None
                )
                try:
                    dc_msgid = bot.rpc.send_msg(accid, chat_id, reply)
                except JsonRpcError as ex:
                    bot.logger.exception(ex)
                    continue
                _cache_put(mb_id, accid, chat_id, dc_msgid)
    elif text:
        for accid, chat_id in chats:
            reply.quoted_message_id = (
                _cache_get(parent_id, accid, chat_id) if parent_id else None
            )
            try:
                dc_msgid = bot.rpc.send_msg(accid, chat_id, reply)
            except JsonRpcError as ex:
                bot.logger.exception(ex)
                continue
            _cache_put(mb_id, accid, chat_id, dc_msgid)


def listen_to_matterbridge(bot: Bot) -> None:
    """Process forever the streams of messages from matterbridge API"""
    bot.logger.debug("Listening to matterbridge API...")
    api_url = mb_config["api"]["url"]
    with requests.Session() as session:
        while True:
            try:
                # use the /api/messages endpoint because /api/stream have issues:
                # https://github.com/42wim/matterbridge/issues/1983
                with session.get(
                    api_url + "/api/messages", headers=_mb_headers()
                ) as resp:
                    for msg in resp.json():
                        bot.logger.debug(msg)
                        mb2dc(bot, msg)
                time.sleep(1)
            except Exception as ex:  # pylint: disable=W0703
                bot.logger.exception(ex)
                time.sleep(15)


_IMAP_CLEANUP_INTERVAL = 1800


def imap_cleanup_loop(bot: Bot) -> None:
    """Periodically expunge fetched messages from the bot's IMAP inbox to bound
    server-side mailbox growth; DC keeps a local copy for delete_device_after."""
    while True:
        time.sleep(_IMAP_CLEANUP_INTERVAL)
        for accid in bot.rpc.get_all_account_ids():
            try:
                _imap_expunge_seen(bot, accid)
            except (imaplib.IMAP4.error, OSError, ValueError, JsonRpcError) as ex:
                bot.logger.warning("IMAP cleanup failed for account %s: %s", accid, ex)


def _imap_expunge_seen(bot: Bot, accid: int) -> None:
    host = bot.rpc.get_config(accid, "configured_mail_server")
    port_raw = bot.rpc.get_config(accid, "configured_mail_port") or "993"
    user = bot.rpc.get_config(accid, "configured_mail_user")
    pw = bot.rpc.get_config(accid, "configured_mail_pw")
    if not (host and user and pw):
        return
    with imaplib.IMAP4_SSL(host, int(port_raw)) as imap:
        imap.login(user, pw)
        imap.select("INBOX")
        typ, data = imap.uid("SEARCH", "SEEN")
        if typ != "OK" or not data or not data[0]:
            return
        uids = data[0].decode("ascii").replace(" ", ",")
        imap.uid("STORE", uids, "+FLAGS", "(\\Deleted)")
        imap.expunge()
