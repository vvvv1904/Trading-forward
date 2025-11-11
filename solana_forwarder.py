#!/usr/bin/env python3
"""
Solana address forwarder (robust version)

- Listens to one or more Telegram channels (by username or numeric -100... ID)
- Detects Solana addresses (Base58 32-44 chars)
- Deduplicates addresses persistently (SQLite) so duplicates are NOT re-sent
- Sends detected addresses to a target trading bot (by username)
- Logs events and errors
"""

import os
import re
import sqlite3
import logging
import asyncio
from typing import List, Union
from telethon import TelegramClient, events, errors

# --------------------
# CONFIG — edit these or set as environment variables
# --------------------
API_ID = int(os.getenv("API_ID", "29320735"))           # replace or export
API_HASH = os.getenv("API_HASH", "8fd644cefa5e3e644f913afb594ec01d")
SESSION_NAME = os.getenv("SESSION_NAME", "session_name")  # Telethon session filename
# Provide target channels as comma-separated list. Examples:
# - single numeric ID: -1002177594166
# - single username: @somechannel
# - multiple: -1002177594166,@AnotherChannel
RAW_TARGET_CHANNELS = os.getenv("TARGET_CHANNELS", "-1002604509392") 
TRADING_BOT = os.getenv("TRADING_BOT", "WizGandalfBot")  # destination bot username or id
SQLITE_DB = os.getenv("SQLITE_DB", "forwarder_state.db")
LOGFILE = os.getenv("LOGFILE", "relay.log")

# Regex to detect base58-like Solana addresses (32-44 chars, excludes 0OIl)
# Characters: 1-9 A-H J-N P-Z a-k m-z (excludes 0,O,I,l)
SOL_ADDR_RE = re.compile(r'\b[1-9A-HJ-NP-Za-km-z]{32,44}\b')

# --------------------
# Logging
# --------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOGFILE, encoding='utf-8')
    ]
)
log = logging.getLogger("solana-forwarder")

# --------------------
# Simple SQLite-backed dedupe store
# --------------------
class DedupeStore:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.execute("""CREATE TABLE IF NOT EXISTS sent_addresses (
                                address TEXT PRIMARY KEY,
                                first_seen_ts INTEGER,
                                message_id INTEGER,
                                channel TEXT
                              );""")
        self._conn.commit()

    def already_sent(self, address: str) -> bool:
        cur = self._conn.execute("SELECT 1 FROM sent_addresses WHERE address = ? LIMIT 1;", (address,))
        return cur.fetchone() is not None

    def mark_sent(self, address: str, ts: int, msg_id: int, channel: str):
        try:
            self._conn.execute(
                "INSERT OR IGNORE INTO sent_addresses(address, first_seen_ts, message_id, channel) VALUES (?, ?, ?, ?);",
                (address, ts, msg_id, channel)
            )
            self._conn.commit()
        except Exception as e:
            log.exception("SQLite insert failed: %s", e)

    def close(self):
        try:
            self._conn.close()
        except:
            pass

# --------------------
# Utility: parse channel list
# --------------------
def parse_target_channels(raw: str) -> List[Union[int, str]]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    out = []
    for p in parts:
        # numeric channel id (starts with -100 or digits)
        try:
            if p.startswith("-100") or (p.lstrip("-").isdigit() and len(p) > 6):
                out.append(int(p))
            else:
                out.append(p)  # username (with or without @)
        except Exception:
            out.append(p)
    return out

# --------------------
# Main logic
# --------------------
async def main():
    targets = parse_target_channels(RAW_TARGET_CHANNELS)
    log.info("Starting Solana forwarder")
    log.info("Targets: %s", targets)
    log.info("Trading bot destination: %s", TRADING_BOT)

    # Initialize dedupe store
    dedupe = DedupeStore(SQLITE_DB)

    # Telethon client
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

    @client.on(events.NewMessage(chats=targets))
    async def new_msg_handler(event: events.NewMessage.Event):
        try:
            text = event.raw_text or ""
            if not text:
                return

            # find addresses in message
            matches = SOL_ADDR_RE.findall(text)
            if not matches:
                return

            # prefer first reasonable match (you can change logic here)
            for addr in matches:
                addr = addr.strip()
                if len(addr) < 32 or len(addr) > 44:
                    continue
                # dedupe
                if dedupe.already_sent(addr):
                    log.debug("Address already sent before: %s", addr)
                    continue

                # Optional: extra safe validation step if you have a validator for Solana (not included)
                # send to trading bot
                try:
                    # send the raw address (or message if you prefer)
                    await client.send_message(TRADING_BOT, addr)
                    log.info("Forwarded address %s -> %s (msg %s, channel %s)",
                             addr, TRADING_BOT, getattr(event.message, 'id', None), str(event.chat_id))

                    # mark as sent
                    dedupe.mark_sent(addr, int(event.message.date.timestamp()), getattr(event.message, 'id', 0), str(event.chat_id))
                except errors.FloodWaitError as f:
                    log.warning("FloodWait error while sending; sleeping for %s seconds", f.seconds)
                    await asyncio.sleep(f.seconds + 1)
                    # after sleep, attempt resend once
                    try:
                        await client.send_message(TRADING_BOT, addr)
                        dedupe.mark_sent(addr, int(event.message.date.timestamp()), getattr(event.message, 'id', 0), str(event.chat_id))
                        log.info("Resent after FloodWait: %s", addr)
                    except Exception as e:
                        log.exception("Failed to resend after wait: %s", e)
                except Exception as e:
                    log.exception("Failed to forward address %s: %s", addr, e)

        except Exception as e:
            log.exception("Handler error: %s", e)

    # start client
    try:
        await client.start()
        log.info("Client started (session stored as %s.session)", SESSION_NAME)
        # keeps running until disconnected
        await client.run_until_disconnected()
    except Exception as e:
        log.exception("Client failed: %s", e)
    finally:
        dedupe.close()
        await client.disconnect()
        log.info("Shutdown complete")

# --------------------
# Entrypoint
# --------------------
if __name__ == "__main__":
    # Quick safety: ensure API_ID / API_HASH present
    if not API_ID or not API_HASH:
        log.error("API_ID/API_HASH not set. Edit top of file or set environment variables.")
        raise SystemExit(1)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Interrupted by user, exiting.")
