#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Интерактивная генерация Telethon StringSession для пользовательского аккаунта.
Читает TELEGRAM_API_ID и TELEGRAM_API_HASH из .env (или запрашивает в консоли).
Итоговую строку сессии выводит в stdout — сохраните её в STRING_SESSION_SALES и т.п.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.sessions import StringSession

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def _api_id() -> int:
    raw = os.getenv("TELEGRAM_API_ID")
    if raw and raw.strip().isdigit():
        return int(raw.strip())
    s = input("TELEGRAM_API_ID (число): ").strip()
    return int(s)


def _api_hash() -> str:
    h = (os.getenv("TELEGRAM_API_HASH") or "").strip()
    if h:
        return h
    return input("TELEGRAM_API_HASH: ").strip()


def _phone() -> str:
    return input("Номер телефона (с кодом страны, например +79001234567): ").strip()


def _code() -> str:
    return input("Код подтверждения из Telegram / SMS: ").strip()


def _password() -> str | None:
    s = input("Пароль 2FA (если нет — просто Enter): ").strip()
    return s if s else None


async def main() -> None:
    try:
        api_id = _api_id()
        api_hash = _api_hash()
    except ValueError as e:
        logger.error("Некорректный API ID: %s", e)
        sys.exit(1)

    if not api_hash:
        logger.error("API Hash пустой")
        sys.exit(1)

    try:
        async with TelegramClient(StringSession(), api_id, api_hash) as client:
            await client.start(
                phone=_phone,
                code_callback=_code,
                password=_password,
            )
            saved = client.session.save()
        print()
        print("========== StringSession (сохраните в секрет / .env) ==========")
        print(saved)
        print("================================================================")
        logger.info("Готово. Не публикуйте эту строку в открытых репозиториях.")
    except Exception as e:
        logger.exception("Ошибка авторизации: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
