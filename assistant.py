#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram userbot (Telethon): sales AI assistant via Google Gemini API.
Reads STRING_SESSION_SALES, answers private incoming DMs, tracks history in memory.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("assistant")

STRING_SESSION_SALES = os.getenv("STRING_SESSION_SALES")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
OWNER_CHAT_ID_RAW = os.getenv("OWNER_CHAT_ID")
TELEGRAM_API_ID_RAW = os.getenv("TELEGRAM_API_ID")
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")

GEMINI_GENERATE_URL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.5-flash:generateContent"
)

MAX_HISTORY_TURNS = 30
MAX_TOKENS_REPLY = 1024
MAX_TOKENS_CLASSIFY = 64

SYSTEM_PERSONA = """Ты — Александр Гребенщиков, эксперт по автоматизации бизнеса с ИИ. Помогаешь предпринимателям убрать рутину. Пиши коротко, по-человечески, без продажного тона. Первое сообщение — короткое знакомство и один вопрос про их боль. Дальше — слушай, уточняй, показывай экспертизу. Кейсы упоминай только если разговор тёплый."""

# user_id -> list of {"role": "user"|"assistant", "content": str}
conversations: Dict[int, List[Dict[str, str]]] = {}


def _require_env() -> None:
    missing = []
    if not STRING_SESSION_SALES:
        missing.append("STRING_SESSION_SALES")
    if not GEMINI_API_KEY:
        missing.append("GEMINI_API_KEY")
    if not TELEGRAM_API_ID_RAW:
        missing.append("TELEGRAM_API_ID")
    if not TELEGRAM_API_HASH:
        missing.append("TELEGRAM_API_HASH")
    if missing:
        raise RuntimeError(f"Missing env: {', '.join(missing)}")


def _owner_chat_id() -> Optional[int]:
    if not OWNER_CHAT_ID_RAW:
        logger.warning("OWNER_CHAT_ID not set; HOT lead notifications disabled")
        return None
    try:
        return int(OWNER_CHAT_ID_RAW.strip())
    except ValueError:
        logger.error("OWNER_CHAT_ID must be an integer Telegram user id")
        return None


def _trim_history(uid: int) -> None:
    hist = conversations.get(uid, [])
    max_msgs = MAX_HISTORY_TURNS * 2
    if len(hist) > max_msgs:
        conversations[uid] = hist[-max_msgs:]


def _messages_to_gemini_contents(
    messages: List[Dict[str, str]],
) -> List[Dict[str, Any]]:
    """Internal roles user|assistant -> Gemini user|model."""
    contents: List[Dict[str, Any]] = []
    for m in messages:
        role = m.get("role", "user")
        text = m.get("content") or ""
        gemini_role = "model" if role == "assistant" else "user"
        contents.append({"role": gemini_role, "parts": [{"text": text}]})
    return contents


async def gemini_generate_content(
    http: httpx.AsyncClient,
    *,
    system_instruction: str,
    conversation_messages: List[Dict[str, str]],
    max_output_tokens: int,
) -> str:
    payload: Dict[str, Any] = {
        "systemInstruction": {
            "parts": [{"text": system_instruction}],
        },
        "contents": _messages_to_gemini_contents(conversation_messages),
        "generationConfig": {
            "maxOutputTokens": max_output_tokens,
        },
    }
    params = {"key": GEMINI_API_KEY or ""}
    resp = await http.post(
        GEMINI_GENERATE_URL,
        params=params,
        json=payload,
        headers={"content-type": "application/json"},
        timeout=120.0,
    )
    resp.raise_for_status()
    data = resp.json()
    candidates = data.get("candidates") or []
    if not candidates:
        logger.warning("Gemini returned no candidates: %s", data)
        return ""
    content = candidates[0].get("content") or {}
    parts = content.get("parts") or []
    texts: List[str] = []
    for p in parts:
        if isinstance(p, dict) and "text" in p:
            texts.append(p.get("text") or "")
    return "".join(texts).strip()


async def generate_reply(http: httpx.AsyncClient, uid: int) -> str:
    msgs = conversations.get(uid, [])
    return await gemini_generate_content(
        http,
        system_instruction=SYSTEM_PERSONA,
        conversation_messages=msgs,
        max_output_tokens=MAX_TOKENS_REPLY,
    )


async def classify_lead_temperature(http: httpx.AsyncClient, uid: int) -> str:
    """Returns HOT or NORMAL (fallback NORMAL on parse errors)."""
    hist = conversations.get(uid, [])
    lines: List[str] = []
    for m in hist[-20:]:
        role = m.get("role", "")
        content = (m.get("content") or "").replace("\n", " ").strip()
        lines.append(f"{role}: {content}")
    transcript = "\n".join(lines)

    system = (
        "You classify B2B chat leads. Reply with exactly one word: HOT or NORMAL.\n"
        "HOT = clear buying / next-step intent: price, cost, сколько стоит, ТЗ, "
        "техзадание, когда можем начать, давайте начнём, договор, оплата, счёт, сроки в контексте покупки.\n"
        "NORMAL = everything else (small talk, vague interest, no ask to proceed)."
    )
    user_msg = f"Conversation:\n{transcript}\n\nClassification:"
    raw = await gemini_generate_content(
        http,
        system_instruction=system,
        conversation_messages=[{"role": "user", "content": user_msg}],
        max_output_tokens=MAX_TOKENS_CLASSIFY,
    )
    token = raw.upper().split()
    for t in token:
        if "HOT" in t:
            return "HOT"
    return "NORMAL"


def format_lead_header(sender: User) -> str:
    name = f"{sender.first_name or ''} {sender.last_name or ''}".strip() or "(без имени)"
    un = f"@{sender.username}" if sender.username else "username: —"
    return f"Горячий лид\nID: {sender.id}\n{un}\nИмя: {name}"


def format_history_for_owner(uid: int) -> str:
    lines: List[str] = ["--- История ---"]
    for m in conversations.get(uid, []):
        tag = "Лид" if m["role"] == "user" else "Ассистент"
        lines.append(f"{tag}: {m.get('content', '')}")
    return "\n".join(lines)


async def notify_owner_hot(
    client: TelegramClient,
    owner_id: int,
    sender: User,
    uid: int,
) -> None:
    header = format_lead_header(sender)
    body = format_history_for_owner(uid)
    text = f"{header}\n\n{body}"
    if len(text) > 3500:
        text = text[:3490] + "\n…(обрезано)"
    try:
        await client.send_message(owner_id, text)
        logger.info("Sent HOT notification to owner for user_id=%s", uid)
    except Exception as e:
        logger.exception("Failed to notify owner for user_id=%s: %s", uid, e)


async def assistant_main() -> None:
    _require_env()
    owner_id = _owner_chat_id()
    api_id = int(TELEGRAM_API_ID_RAW or "0")

    client = TelegramClient(
        StringSession(STRING_SESSION_SALES),
        api_id,
        TELEGRAM_API_HASH,
    )

    async with httpx.AsyncClient() as http:
        async with client:
            me = await client.get_me()
            if not me:
                logger.error("Could not get self user")
                return
            my_id = me.id
            logger.info("Logged in as id=%s", my_id)

            @client.on(events.NewMessage(incoming=True))
            async def on_incoming(event: events.NewMessage.Event) -> None:
                try:
                    if not event.is_private:
                        return
                    msg = event.message
                    if not msg or not msg.text:
                        return

                    sender = await event.get_sender()
                    if not isinstance(sender, User):
                        return
                    if sender.bot:
                        logger.debug("Skip bot sender id=%s", sender.id)
                        return
                    if sender.id == my_id:
                        return

                    uid = sender.id
                    text = msg.text.strip()
                    if not text:
                        return

                    logger.info("Incoming DM from user_id=%s len=%s", uid, len(text))

                    conversations.setdefault(uid, []).append(
                        {"role": "user", "content": text}
                    )
                    _trim_history(uid)

                    try:
                        reply = await generate_reply(http, uid)
                    except Exception as e:
                        logger.exception("Gemini reply failed for user_id=%s: %s", uid, e)
                        return

                    if not reply:
                        logger.warning("Empty reply from Gemini for user_id=%s", uid)
                        return

                    conversations[uid].append({"role": "assistant", "content": reply})
                    _trim_history(uid)

                    try:
                        await event.respond(reply)
                    except Exception as e:
                        logger.exception("Failed to send Telegram reply user_id=%s: %s", uid, e)
                        return

                    try:
                        temperature = await classify_lead_temperature(http, uid)
                    except Exception as e:
                        logger.exception("Gemini classify failed user_id=%s: %s", uid, e)
                        temperature = "NORMAL"

                    logger.info("Lead temperature user_id=%s -> %s", uid, temperature)

                    if temperature == "HOT" and owner_id is not None:
                        await notify_owner_hot(client, owner_id, sender, uid)

                except Exception as e:
                    logger.exception("on_incoming error: %s", e)

            logger.info("Client started; listening for private messages…")
            await client.run_until_disconnected()


if __name__ == "__main__":
    try:
        asyncio.run(assistant_main())
    except KeyboardInterrupt:
        logger.info("Interrupted")
    except Exception as e:
        logger.exception("Fatal: %s", e)
        sys.exit(1)
