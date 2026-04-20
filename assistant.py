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
import random
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import User
from bot import stopped_users, sheets_manager

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

SYSTEM_PERSONA = """Ты — Александр Гребенщиков. Автоматизируешь бизнес с помощью ИИ и n8n. Работаешь с предпринимателями, фрилансерами, владельцами малого бизнеса.

СТИЛЬ ОБЩЕНИЯ:
- Пиши коротко. Максимум 3-4 предложения за раз. Никаких длинных текстов.
- Общайся на ВЫ. Всегда. Независимо от того как пишет собеседник.
- Один вопрос за раз. Не задавай несколько вопросов подряд.
- Разговорный тон. Как будто пишешь другу-предпринимателю, не клиенту.
- Никаких маркированных списков со звёздочками или тире в первых сообщениях.
- Никакого официального языка. Никаких "уточните", "предоставим", "осуществим".
- Цель — выйти на созвон 15-20 минут. Предлагай его когда человек проявил интерес.

ТВОИ КЕЙСЫ (используй когда уместно, не перечисляй все сразу):
- AI-бухгалтер в Telegram: предприниматель пишет операцию текстом → данные сами идут в таблицу, резервируется налог, напоминания об авансах
- Парсер Telegram-групп: одна команда → до 10 групп за раз → имена, username, телефоны в таблицу
- AI Digest Bot: каждое утро сводка по нише из десятков источников → 3 минуты и в курсе рынка
- Парсер ВК конкурентов: ER, просмотры, лайки по любым сообществам → в Google Sheets, автообновление
- AI-ассистент аккаунта: отвечает в личке 24/7, прогревает, передаёт тебе только горячих

ЛОГИКА ДИАЛОГА:
Ты ведёшь продажный диалог как опытный менеджер.
Никогда не торопись. Сначала человек должен почувствовать
что его понимают — только потом предлагай следующий шаг.

ФАЗЫ ДИАЛОГА:

Фаза 1 — ЗНАКОМСТВО (1-2 сообщения):
После приветствия представься и узнай чем занимается.
"Меня Александр зовут, занимаюсь автоматизацией бизнеса с ИИ.
А вы чем занимаетесь?"
Если ответил коротко — уточни: своё дело или найм.

Фаза 2 — ПОНЯТЬ СИТУАЦИЮ (2-3 сообщения):
Узнай как устроен бизнес. Задавай по одному вопросу:
- Сколько человек в команде?
- Как сейчас ведёте учёт / общаетесь с клиентами /
  находите заказы?
- Что делаете руками которое хочется автоматизировать?
Реагируй живо на каждый ответ: "Понял", "Знакомая история",
"Часто с этим сталкиваюсь". Показывай что слушаешь.

Фаза 3 — БОЛЬ (1-2 сообщения):
Когда понял ситуацию — копни глубже:
"А что из этого больше всего времени съедает?"
"И как давно так работаете?"
Дай человеку выговориться. Не перебивай предложениями.

Фаза 4 — ЭКСПЕРТИЗА (1-2 сообщения):
Только когда боль понята — покажи что знаешь решение.
Упомяни похожий кейс коротко, без деталей:
"У меня был похожий клиент в [похожей нише] —
убрали эту рутину за 2 недели."
Не перечисляй все возможности. Один конкретный пример.

Фаза 5 — СОЗВОН (после 6-8 сообщений минимум):
Только когда человек вовлечён и сам задаёт вопросы —
предлагай созвон:
"Давайте на 15-20 минут созвонимся —
покажу конкретно как это будет работать у вас.
Когда удобно?"
Если отказался от созвона — не дави. Спроси что мешает,
продолжай диалог.

Фаза 6 — ГОРЯЧИЙ ЛИД:
Человек сам спросил цену / прислал ТЗ / сказал "давайте" —
это HOT. Скажи что уточнишь детали и свяжешься.

ВАЖНЫЕ ПРАВИЛА:
- Никогда не предлагай созвон раньше чем после 6 сообщений
- Если человек отвечает односложно — задай уточняющий вопрос
- Если человек задаёт вопрос про услуги — отвечай коротко
  и задай встречный вопрос про их ситуацию
- Реакции перед вопросами: "Понял.", "Огонь.",
  "Знакомая история.", "Интересно.", "Это часто бывает."
- Никогда не пиши больше 3 предложений подряд
- Один вопрос за раз, всегда
- Если человек сказал что работает В НАЙМЕ — не закрывай диалог.
  Уточни его роль и влияние:
  "Понял. А вы в какой роли — можете влиять на процессы
  или решения принимает кто-то другой?"

  Если он ЛПР (руководитель, директор, head of, менеджер
  с влиянием на процессы) — продолжай диалог как обычно,
  он может стать клиентом или привести компанию.

  Если он рядовой сотрудник без влияния на решения —
  заверши мягко:
  "Понял. Если захотите предложить руководству —
  могу подготовить короткое описание что можно автоматизировать
  в вашей компании. Это бесплатно, просто напишите."
  Это даёт шанс что он передаст выше.
  Статус в Sheets — "Найм" но не останавливай диалог полностью.

ЗАПРЕЩЕНО:
- Писать больше 4 предложений подряд
- Задавать 2+ вопроса в одном сообщении
- Использовать слова: "уточните", "предоставим", "осуществим", "данный", "является"
- Перечислять все кейсы списком без запроса
- Говорить "Я — языковая модель" или намекать что ты бот"""

# user_id -> list of {"role": "user"|"assistant", "content": str}
conversations: Dict[int, List[Dict[str, str]]] = {}
COLD_MARKERS = ["не интересно", "не надо", "отписка", "стоп", "не актуально", "не нужно", "отстань"]


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


async def load_history_from_telegram(client, uid: int, my_id: int, limit: int = 20):
    """Загружает последние сообщения диалога из Telegram в память"""
    if uid in conversations and len(conversations[uid]) > 0:
        return  # история уже есть, не перезагружаем

    try:
        messages = []
        async for msg in client.iter_messages(uid, limit=limit):
            if not msg.text:
                continue
            role = "assistant" if msg.out else "user"
            messages.append({"role": role, "content": msg.text})

        # Разворачиваем — iter_messages идёт от новых к старым
        messages.reverse()

        if messages:
            conversations[uid] = messages
            logger.info(f"Loaded {len(messages)} messages from Telegram history for user_id={uid}")
    except Exception as e:
        logger.warning(f"Could not load history for user_id={uid}: {e}")


def load_history_from_sheets(uid: int, limit: int = 20):
    try:
        sheet = sheets_manager.spreadsheet.worksheet("История")
        rows = sheet.get_all_values()
        if len(rows) <= 1:
            return
        selected = []
        for row in rows[1:]:
            if len(row) < 5:
                continue
            if row[0].strip() == str(uid):
                selected.append(row)
        if not selected:
            return
        selected = selected[-limit:]
        conversations[uid] = [{"role": r[2], "content": r[3]} for r in selected]
        logger.info("Loaded %s messages from Sheets history for user_id=%s", len(selected), uid)
    except Exception as e:
        logger.warning("Could not load history from sheets for user_id=%s: %s", uid, e)


def save_history_to_sheets(uid: int, username: str, conv: Dict[int, List[Dict[str, str]]]):
    try:
        sheet = sheets_manager.spreadsheet.worksheet("История")
    except Exception:
        try:
            sheet = sheets_manager.spreadsheet.add_worksheet(title="История", rows=2000, cols=5)
            sheet.append_row(["user_id", "username", "role", "content", "timestamp"])
        except Exception as e:
            logger.warning("Could not prepare history sheet: %s", e)
            return

    try:
        recent = conv.get(uid, [])[-20:]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = [[str(uid), username, m.get("role", ""), m.get("content", ""), timestamp] for m in recent]
        if rows:
            sheet.append_rows(rows, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.warning("Could not save history to sheets for user_id=%s: %s", uid, e)


def get_group_context(username: str) -> str:
    if not username:
        return ""
    try:
        sheet = sheets_manager.spreadsheet.worksheet("Контакты")
        cell = sheet.find(f"@{username}")
        if not cell:
            return ""
        row = sheet.row_values(cell.row)
        group_name = row[4] if len(row) > 4 else ""
        if group_name:
            return f"\n\nЛид из группы: {group_name}"
    except Exception as e:
        logger.warning("Could not fetch group context for @%s: %s", username, e)
    return ""


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


async def generate_reply(http: httpx.AsyncClient, uid: int, group_context: str = "") -> str:
    msgs = conversations.get(uid, [])
    return await gemini_generate_content(
        http,
        system_instruction=SYSTEM_PERSONA + group_context,
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
                    if uid in stopped_users:
                        logger.info("Skipping stopped user uid=%s", uid)
                        return

                    await load_history_from_telegram(client, uid, my_id)
                    if not conversations.get(uid):
                        load_history_from_sheets(uid)
                    text = msg.text.strip()
                    if not text:
                        return

                    logger.info("Incoming DM from user_id=%s len=%s", uid, len(text))

                    conversations.setdefault(uid, []).append(
                        {"role": "user", "content": text}
                    )
                    _trim_history(uid)

                    try:
                        group_context = get_group_context(sender.username or "")
                        reply = await generate_reply(http, uid, group_context=group_context)
                    except Exception as e:
                        logger.exception("Gemini reply failed for user_id=%s: %s", uid, e)
                        if owner_id is not None:
                            try:
                                await client.send_message(
                                    owner_id,
                                    f"⚠️ Ассистент не смог ответить @{sender.username or uid}\n"
                                    f"Ошибка: {e}\n"
                                    "Напиши сам!",
                                )
                            except Exception:
                                pass
                        return

                    if not reply:
                        logger.warning("Empty reply from Gemini for user_id=%s", uid)
                        return

                    conversations[uid].append({"role": "assistant", "content": reply})
                    _trim_history(uid)
                    save_history_to_sheets(uid, sender.username or "", conversations)

                    try:
                        # Случайная пауза перед ответом (имитация человека)
                        await asyncio.sleep(random.uniform(2, 5))

                        # Статус "печатает" пропорционально длине ответа
                        typing_time = min(len(reply) * 0.04, 8.0)
                        async with client.action(event.chat_id, 'typing'):
                            await asyncio.sleep(typing_time)

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

                    text_l = text.lower()
                    if any(marker in text_l for marker in COLD_MARKERS):
                        stopped_users.add(uid)
                        try:
                            sheet = sheets_manager.spreadsheet.worksheet("Контакты")
                            if sender.username:
                                cell = sheet.find(f"@{sender.username}")
                                if cell:
                                    sheet.update_cell(cell.row, 7, "COLD")
                        except Exception as e:
                            logger.warning("Could not set COLD status for user_id=%s: %s", uid, e)

                        if owner_id is not None:
                            try:
                                await client.send_message(
                                    owner_id,
                                    f"❄️ @{sender.username or uid} отказал. Диалог остановлен.",
                                )
                            except Exception as e:
                                logger.warning("Could not notify owner about COLD lead: %s", e)
                        return

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
