"""Periodic IMAP cleanup of the bot's inbox.

Keeps server-side mailbox bounded; local copies are retained per
delete_device_after.
"""

import imaplib
import time
from threading import Thread
from typing import Optional, Tuple

from deltachat2 import Bot, JsonRpcError

_INTERVAL_SECONDS = 1800
_INITIAL_DELAY_SECONDS = 30
_DEFAULT_IMAP_PORT = 993


def start(bot: Bot) -> None:
    Thread(target=_loop, args=(bot,), daemon=True).start()


def _loop(bot: Bot) -> None:
    time.sleep(_INITIAL_DELAY_SECONDS)
    while True:
        for accid in bot.rpc.get_all_account_ids():
            try:
                _wipe_inbox(bot, accid)
            except (imaplib.IMAP4.error, OSError, ValueError, JsonRpcError) as ex:
                bot.logger.warning("IMAP cleanup failed for account %s: %s", accid, ex)
        time.sleep(_INTERVAL_SECONDS)


def _wipe_inbox(bot: Bot, accid: int) -> None:
    creds = _imap_creds(bot, accid)
    if creds is None:
        return
    host, port, user, pw = creds
    with imaplib.IMAP4_SSL(host, port) as imap:
        imap.login(user, pw)
        imap.select("INBOX")
        typ, data = imap.uid("SEARCH", "ALL")
        if typ != "OK" or not data or not data[0]:
            return
        uids = data[0].decode("ascii").replace(" ", ",")
        imap.uid("STORE", uids, "+FLAGS", "(\\Deleted)")
        imap.expunge()
        bot.logger.info("IMAP cleanup expunged %d msg(s) for account %s", len(uids.split(",")), accid)


def _imap_creds(bot: Bot, accid: int) -> Optional[Tuple[str, int, str, str]]:
    transports = bot.rpc.list_transports(accid)
    if not transports:
        return None
    transport = transports[0]
    addr = transport.get("addr") or ""
    imap = transport.get("imap") or {}
    host = imap.get("server") or _derive_host(addr)
    port = int(imap.get("port") or 0) or _DEFAULT_IMAP_PORT
    user = imap.get("user") or addr
    pw = imap.get("password") or bot.rpc.get_config(accid, "mail_pw") or ""
    if not (host and user and pw):
        return None
    return host, port, user, pw


def _derive_host(addr: str) -> str:
    domain = addr.partition("@")[2]
    return f"mail.{domain}" if domain else ""
