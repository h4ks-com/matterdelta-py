"""Matterbridge API interaction"""

import base64
import json
import tempfile
import time
from collections import OrderedDict
from pathlib import Path
from threading import Lock, Thread
from typing import Dict, List, Optional, Tuple

import requests
from deltachat2 import Bot, JsonRpcError, Message, MessageViewtype, MsgData

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


def _needs_full_download(msg: Message) -> bool:
    """True when a message has an attachment that hasn't been fetched yet."""
    if not msg.get("file_name"):
        return False
    if msg.get("file"):
        return False
    state = msg.get("download_state")
    if state in (None, "Done"):
        # Some servers (chatmail) don't expose download_state for already-done
        # messages; if there's no file but no state either, try anyway.
        return True
    return state in ("Available", "InProgress", "Failure")


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


def dc2mb(bot: Bot, accid: int, msg: Message) -> None:
    """Send a Delta Chat message to the matterbridge side."""
    if not msg.text and not msg.file:  # ignore buggy empty messages
        return
    # NewMessage can fire before the attachment finishes downloading
    if _needs_full_download(msg):
        try:
            bot.rpc.download_full_msg(accid, msg.id)
            msg = Message(bot.rpc.get_message(accid, msg.id))
        except JsonRpcError as ex:
            bot.logger.warning("download_full_msg failed for msg %s: %s", msg.id, ex)
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
        api_url = mb_config["api"]["url"]
        token = mb_config["api"].get("token", "")
        headers = {"Authorization": f"Bearer {token}"} if token else None
        for gateway in gateways:
            data["gateway"] = gateway
            bot.logger.debug("DC->MB %s", data)
            if api_url:
                try:
                    resp = requests.post(
                        api_url + "/api/message",
                        json=data,
                        headers=headers,
                        timeout=60,
                    )
                    posted = resp.json() if resp.ok else {}
                except (ValueError, requests.RequestException):
                    posted = {}
                # matterbridge assigns an id on POST so other clients can
                # parent_id-reference this DC-originated message; cache it
                # under the canonical "api <id>" form that matterbridge uses
                # for parent_id resolution.
                posted_id = posted.get("id") or ""
                if posted_id:
                    _cache_put(
                        "api " + posted_id, accid, msg.chat_id, msg.id
                    )
            mb2dc(bot, data, (accid, msg.chat_id))


def mb2dc(bot: Bot, msg: dict, exclude: Tuple[int, int] = (0, 0)) -> None:  # noqa: C901
    """Send a message from matterbridge to the bridged Delta Chat group"""
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
    token = mb_config["api"].get("token", "")
    headers = {"Authorization": f"Bearer {token}"} if token else None
    with requests.Session() as session:
        while True:
            try:
                # use the /api/messages endpoint because /api/stream have issues:
                # https://github.com/42wim/matterbridge/issues/1983
                with session.get(api_url + "/api/messages", headers=headers) as resp:
                    for msg in resp.json():
                        bot.logger.debug(msg)
                        mb2dc(bot, msg)
                time.sleep(1)
            except Exception as ex:  # pylint: disable=W0703
                bot.logger.exception(ex)
                time.sleep(15)
