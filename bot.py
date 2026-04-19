#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import asyncio
import time
import random
import logging
import signal
import threading
from datetime import datetime
from typing import List, Dict

from aiohttp import web

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.functions.channels import GetFullChannelRequest

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID'))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
STRING_SESSION = os.getenv('STRING_SESSION')

SHEET_CONTACTS = "Контакты"
SHEET_STATS = "Статистика"

MAX_GROUPS_PER_RUN = 10
DELAY_MIN = 2
DELAY_MAX = 5

DEFAULT_CRITERIA = {
    'max_contacts': 10000,
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

user_data = {}


def get_user_criteria(user_id: int) -> Dict:
    if user_id not in user_data:
        user_data[user_id] = DEFAULT_CRITERIA.copy()
    return user_data[user_id]


def update_user_criteria(user_id: int, key: str, value):
    if user_id not in user_data:
        user_data[user_id] = DEFAULT_CRITERIA.copy()
    user_data[user_id][key] = value


# ─────────────────────────────────────────────
# ФЕЙКОВЫЙ ВЕБ-СЕРВЕР для Render health-check
# ─────────────────────────────────────────────

async def _health(request):
    return web.Response(text="OK")


def _run_web_server():
    port = int(os.environ.get("PORT", 8080))
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    app = web.Application()
    app.router.add_get("/", _health)
    app.router.add_get("/health", _health)
    runner = web.AppRunner(app)
    loop.run_until_complete(runner.setup())
    site = web.TCPSite(runner, "0.0.0.0", port)
    loop.run_until_complete(site.start())
    logger.info(f"Health-check сервер запущен на порту {port}")
    loop.run_forever()


# ─────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────

class GoogleSheetsManager:
    def __init__(self):
        self.client = None
        self.spreadsheet = None

    def connect(self):
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(SPREADSHEET_ID)

            worksheets = self.spreadsheet.worksheets()
            real_names = [ws.title for ws in worksheets]
            logger.info(f"✅ Connected to Google Sheets: '{self.spreadsheet.title}'")
            logger.info(f"📋 Листы в таблице: {real_names}")
            logger.info(f"🔍 Лист контактов '{SHEET_CONTACTS}': {'✅ НАЙДЕН' if SHEET_CONTACTS in real_names else '❌ НЕ НАЙДЕН'}")
            logger.info(f"🔍 Лист статистики '{SHEET_STATS}': {'✅ НАЙДЕН' if SHEET_STATS in real_names else '❌ НЕ НАЙДЕН'}")
            return True
        except Exception as e:
            logger.error(f"❌ Google Sheets error: {e}", exc_info=True)
            return False

    def ensure_headers(self):
        """Проверяет и при необходимости создаёт заголовки в листе Контакты"""
        try:
            sheet = self.spreadsheet.worksheet(SHEET_CONTACTS)
            first_row = sheet.row_values(1)
            expected = ['Username', 'Имя', 'Фамилия', 'Телефон', 'Группа', 'Дата']
            if first_row != expected:
                sheet.insert_row(expected, index=1)
                sheet.format('A1:F1', {'textFormat': {'bold': True}})
                logger.info("✅ Заголовки созданы в листе Контакты")
        except Exception as e:
            logger.error(f"⚠️ Не удалось создать заголовки: {e}")

    def write_contacts(self, contacts: List[Dict]) -> bool:
        if not contacts:
            logger.warning("⚠️ No contacts to write")
            return False
        try:
            if not self.spreadsheet:
                raise RuntimeError("Spreadsheet не подключён")

            logger.info(f"📋 Открываю лист: '{SHEET_CONTACTS}'")
            sheet = self.spreadsheet.worksheet(SHEET_CONTACTS)
            logger.info(f"✅ Лист найден: {sheet.title}")

            # Структура колонок: Username | Имя | Фамилия | Телефон | Группа | Дата
            rows = []
            for contact in contacts:
                row = [
                    contact.get('username', ''),                    # A: Username
                    contact.get('first_name', ''),                  # B: Имя
                    contact.get('last_name', ''),                   # C: Фамилия
                    contact.get('phone', ''),                       # D: Телефон
                    contact.get('group', ''),                       # E: Группа
                    datetime.now().strftime('%Y-%m-%d %H:%M'),      # F: Дата
                ]
                rows.append(row)

            logger.info(f"📝 Записываю {len(rows)} строк...")
            sheet.append_rows(rows, value_input_option='USER_ENTERED')
            logger.info(f"✅ Сохранено {len(contacts)} контактов")
            return True

        except gspread.exceptions.WorksheetNotFound:
            msg = f"Лист '{SHEET_CONTACTS}' не найден. Переименуй вкладку в Google Sheets в: Контакты"
            logger.error(f"❌ WorksheetNotFound: {msg}")
            raise RuntimeError(msg)

        except gspread.exceptions.SpreadsheetNotFound:
            msg = f"Таблица с ID '{SPREADSHEET_ID}' не найдена. Проверь SPREADSHEET_ID в Render."
            logger.error(f"❌ SpreadsheetNotFound: {msg}")
            raise RuntimeError(msg)

        except gspread.exceptions.APIError as e:
            msg = f"Google API ошибка: {e.response.status_code} — {e.args[0]}"
            logger.error(f"❌ APIError: {msg}")
            raise RuntimeError(msg)

        except Exception as e:
            msg = f"{type(e).__name__}: {str(e)}"
            logger.error(f"❌ Ошибка при записи: {msg}", exc_info=True)
            raise RuntimeError(msg)

    def write_stats(self, stats: Dict):
        try:
            sheet = self.spreadsheet.worksheet(SHEET_STATS)
            row = [
                datetime.now().strftime('%Y-%m-%d %H:%M'),
                stats.get('groups_parsed', 0),
                stats.get('total_contacts', 0),
                stats.get('with_username', 0),
                stats.get('with_phone', 0),
                stats.get('bots_skipped', 0),
                stats.get('duration_sec', 0),
            ]
            sheet.append_row(row)
        except gspread.exceptions.WorksheetNotFound:
            logger.error(f"❌ Лист '{SHEET_STATS}' не найден. Создай вкладку 'Статистика' в таблице.")
        except Exception as e:
            logger.error(f"❌ Ошибка записи статистики: {type(e).__name__}: {e}")


sheets_manager = GoogleSheetsManager()


# ─────────────────────────────────────────────
# TELETHON ПАРСЕР
# ─────────────────────────────────────────────

class TelegramParser:
    def __init__(self):
        self.client = None

    async def connect(self):
        try:
            logger.info("🔌 Creating Telethon client...")
            self.client = TelegramClient(
                StringSession(STRING_SESSION),
                TELEGRAM_API_ID,
                TELEGRAM_API_HASH
            )
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logger.error("❌ Telethon session is not authorized!")
                return False
            logger.info("✅ Connected to Telegram via Telethon")
            return True
        except Exception as e:
            logger.error(f"❌ Telegram connection error: {e}", exc_info=True)
            return False

    async def parse_group(self, group_link: str, max_contacts: int) -> List[Dict]:
        contacts = []
        logger.info("=" * 60)
        logger.info(f"🎯 PARSING GROUP: {group_link}")
        logger.info(f"📊 Max contacts limit: {max_contacts}")

        try:
            logger.info(f"📡 Getting entity for {group_link}...")
            entity = await self.client.get_entity(group_link)
            entity_title = entity.title if hasattr(entity, 'title') else 'Unnamed'
            logger.info(f"✅ Got entity: {entity_title}")

            if hasattr(entity, 'broadcast') and entity.broadcast:
                logger.info("📢 This is a CHANNEL, looking for discussion group...")
                try:
                    full = await self.client(GetFullChannelRequest(channel=entity))
                    if full.full_chat.linked_chat_id:
                        logger.info(f"✅ Found discussion group (ID: {full.full_chat.linked_chat_id})")
                        entity = await self.client.get_entity(full.full_chat.linked_chat_id)
                        entity_title = entity.title if hasattr(entity, 'title') else 'Unnamed'
                        logger.info(f"✅ Switched to discussion group: {entity_title}")
                    else:
                        logger.warning("❌ Channel has NO linked discussion group")
                        return []
                except Exception as e:
                    logger.error(f"❌ Error accessing discussion group: {e}", exc_info=True)
                    return []
            else:
                logger.info("👥 This is a GROUP (not a channel)")

            logger.info("📥 Requesting participants from Telegram...")
            participants = await self.client.get_participants(entity)
            logger.info(f"✅ Telegram returned {len(participants)} total participants")

            if len(participants) > max_contacts:
                logger.info(f"✂️ Limiting to first {max_contacts} participants")
                participants = participants[:max_contacts]

            logger.info(f"🔄 Processing {len(participants)} participants...")

            bots_skipped = 0
            deleted_skipped = 0
            empty_skipped = 0

            for idx, user in enumerate(participants, 1):
                # Пропускаем удалённые аккаунты
                if user.deleted:
                    deleted_skipped += 1
                    continue

                # Пропускаем ботов — они не нужны
                if user.bot:
                    bots_skipped += 1
                    continue

                # Пропускаем совсем пустые аккаунты (нет ни username ни имени)
                if not user.username and not user.first_name:
                    empty_skipped += 1
                    continue

                contact = {
                    'username': f"@{user.username}" if user.username else "",
                    'first_name': user.first_name or "",
                    'last_name': user.last_name or "",
                    'phone': f"+{user.phone}" if user.phone else "",
                    'group': group_link,
                }
                contacts.append(contact)

                if idx % 100 == 0:
                    logger.info(f"   📦 Processed {idx}/{len(participants)}...")

                await asyncio.sleep(0.05)

            logger.info("=" * 60)
            logger.info(f"✅ PARSING COMPLETE!")
            logger.info(f"   📊 Полезных контактов: {len(contacts)}")
            logger.info(f"   🤖 Ботов пропущено: {bots_skipped}")
            logger.info(f"   🗑️ Удалённых аккаунтов: {deleted_skipped}")
            logger.info(f"   👻 Пустых аккаунтов: {empty_skipped}")
            logger.info(f"   👤 С username: {sum(1 for c in contacts if c['username'])}")
            logger.info(f"   📱 С телефоном: {sum(1 for c in contacts if c['phone'])}")
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"❌ CRITICAL ERROR parsing {group_link}:", exc_info=True)
            logger.error(f"   Error type: {type(e).__name__}")
            logger.error(f"   Error message: {str(e)}")

        return contacts

    async def disconnect(self):
        if self.client:
            try:
                await self.client.disconnect()
                logger.info("🔌 Disconnected from Telegram")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")


parser = TelegramParser()


# ─────────────────────────────────────────────
# ХЭНДЛЕРЫ БОТА
# ─────────────────────────────────────────────

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я парсер Telegram групп.\n\n"
        "Команда: /parse @groupname\n"
        "Пример: /parse @python\n\n"
        "Можно несколько групп сразу:\n"
        "/parse @group1 @group2 @group3\n\n"
        "Боты и удалённые аккаунты фильтруются автоматически."
    )


async def parse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id

    if not context.args:
        await update.message.reply_text("❌ Укажи группу!\n\nПример: /parse @groupname")
        return

    groups_str = ' '.join(context.args)
    groups = [g.strip() for g in groups_str.replace(',', ' ').split() if g.strip()]

    if len(groups) > MAX_GROUPS_PER_RUN:
        await update.message.reply_text(f"⚠️ Максимум {MAX_GROUPS_PER_RUN} групп за раз!")
        return

    criteria = get_user_criteria(user_id)

    keyboard = [
        [InlineKeyboardButton(f"📊 Лимит: {criteria['max_contacts']} контактов", callback_data="adj")],
        [InlineKeyboardButton("🚀 ПАРСИТЬ!", callback_data=f"go:{','.join(groups)}")],
    ]

    await update.message.reply_text(
        f"📋 Настройки парсинга:\n"
        f"📊 Макс. контактов: {criteria['max_contacts']}\n"
        f"🚫 Боты: фильтруются автоматически\n\n"
        f"Группы: {', '.join(groups)}",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data

    if data.startswith("go:"):
        groups_str = data.split(":", 1)[1]
        groups = [g.strip() for g in groups_str.split(',')]
        await do_parsing(query, user_id, groups)

    elif data == "adj":
        criteria = get_user_criteria(user_id)
        new_val = 1000 if criteria['max_contacts'] >= 10000 else criteria['max_contacts'] + 1000
        update_user_criteria(user_id, 'max_contacts', new_val)

        text = query.message.text
        groups_line = [l for l in text.split('\n') if 'Группы:' in l]
        groups_str = groups_line[0].split(':', 1)[1].strip() if groups_line else ""

        keyboard = [
            [InlineKeyboardButton(f"📊 Лимит: {new_val} контактов", callback_data="adj")],
            [InlineKeyboardButton("🚀 ПАРСИТЬ!", callback_data=f"go:{groups_str}")],
        ]

        await query.edit_message_text(
            f"📋 Настройки парсинга:\n"
            f"📊 Макс. контактов: {new_val}\n"
            f"🚫 Боты: фильтруются автоматически\n\n"
            f"Группы: {groups_str}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def do_parsing(query, user_id: int, groups: List[str]):
    logger.info("#" * 60)
    logger.info("🚀 PARSING SESSION STARTED")
    logger.info(f"👤 User ID: {user_id}")
    logger.info(f"📝 Groups: {', '.join(groups)}")
    logger.info("#" * 60)

    await query.edit_message_text("🚀 Подключаюсь к Telegram...")

    try:
        if not parser.client or not parser.client.is_connected():
            logger.info("🔄 Reconnecting to Telegram...")
            if parser.client:
                try:
                    await parser.client.disconnect()
                except:
                    pass
            parser.client = None

            if not await parser.connect():
                await query.edit_message_text("❌ Ошибка подключения к Telegram!")
                return

        criteria = get_user_criteria(user_id)
        all_contacts = []
        start_time = time.time()

        for idx, group in enumerate(groups, 1):
            logger.info(f"\n{'~' * 60}")
            logger.info(f"📡 Parsing group {idx}/{len(groups)}: {group}")
            logger.info(f"{'~' * 60}")

            await query.edit_message_text(f"📡 Парсинг {idx}/{len(groups)}: {group}...")

            contacts = await parser.parse_group(group, criteria['max_contacts'])
            all_contacts.extend(contacts)

            logger.info(f"➕ Added {len(contacts)} contacts from {group}")
            logger.info(f"📊 Total so far: {len(all_contacts)}")

            await query.edit_message_text(
                f"✅ {group}: {len(contacts)} контактов\n"
                f"📊 Всего собрано: {len(all_contacts)}"
            )

            if idx < len(groups):
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                logger.info(f"⏳ Waiting {delay:.1f}s...")
                await asyncio.sleep(delay)

        if all_contacts:
            logger.info("💾 Saving to Google Sheets...")
            await query.edit_message_text("💾 Сохраняю в Google Sheets...")

            sheets_ok = False
            try:
                sheets_manager.ensure_headers()
                sheets_ok = sheets_manager.write_contacts(all_contacts)
            except RuntimeError as e:
                error_text = (
                    f"⚠️ Контакты собраны ({len(all_contacts)} шт), "
                    f"но запись в Google Sheets не удалась!\n\n"
                    f"❌ Причина:\n<code>{str(e)}</code>\n\n"
                    f"Что сделать: открой Google Sheets и убедись что вкладка называется точно <code>Контакты</code>"
                )
                await query.edit_message_text(error_text, parse_mode="HTML")
                logger.error(f"Sheets write failed: {e}")
                return

            if sheets_ok:
                duration = int(time.time() - start_time)
                stats = {
                    'groups_parsed': len(groups),
                    'total_contacts': len(all_contacts),
                    'with_username': sum(1 for c in all_contacts if c.get('username')),
                    'with_phone': sum(1 for c in all_contacts if c.get('phone')),
                    'bots_skipped': 0,
                    'duration_sec': duration,
                }
                sheets_manager.write_stats(stats)

                result = (
                    f"✅ Парсинг завершён!\n\n"
                    f"📊 Результаты:\n"
                    f"• Групп: {len(groups)}\n"
                    f"• Контактов записано: {len(all_contacts)}\n"
                    f"• С username: {sum(1 for c in all_contacts if c.get('username'))}\n"
                    f"• С телефоном: {sum(1 for c in all_contacts if c.get('phone'))}\n"
                    f"• Время: {duration} сек\n\n"
                    f"📋 Данные в Google Sheets!"
                )
                await query.edit_message_text(result)
        else:
            await query.edit_message_text(
                "⚠️ Парсинг завершён, но контактов не найдено.\n\n"
                "Возможные причины:\n"
                "• Группа закрытая или приватная\n"
                "• В группе только боты\n"
                "• Аккаунт не имеет доступа к участникам"
            )

        logger.info("#" * 60)
        logger.info("✅ PARSING SESSION COMPLETED")
        logger.info(f"📊 Total contacts: {len(all_contacts)}")
        logger.info(f"⏱️ Duration: {int(time.time() - start_time)}s")
        logger.info("#" * 60)

    except Exception as e:
        logger.error("!" * 60)
        logger.error("❌ CRITICAL ERROR in do_parsing")
        logger.error(f"Error: {type(e).__name__}: {str(e)}", exc_info=True)
        logger.error("!" * 60)
        await query.edit_message_text(f"❌ Критическая ошибка: {str(e)}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("🔗 Connecting to Google Sheets...")
    if not sheets_manager.connect():
        logger.error("❌ Failed to connect to Google Sheets!")
        return

    web_thread = threading.Thread(target=_run_web_server, daemon=True)
    web_thread.start()

    logger.info("🤖 Starting Telegram bot...")
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("parse", parse_command))
    app.add_handler(CallbackQueryHandler(button_callback))

    def _handle_sigterm():
        logger.warning("⚠️ SIGTERM получен — завершаю работу...")
        asyncio.create_task(app.stop())

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
    loop.add_signal_handler(signal.SIGINT, _handle_sigterm)

    logger.info("✅ Bot is running and ready!")
    logger.info("=" * 60)

    app.run_polling(
        drop_pending_updates=True,
        timeout=20,
        allowed_updates=Update.ALL_TYPES,
        close_loop=False,
    )


if __name__ == "__main__":
    main()
