#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import asyncio
import time
import random
import logging
from datetime import datetime
from typing import List, Dict

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes

from telethon import TelegramClient
from telethon.errors import FloodWaitError

import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
TELEGRAM_API_ID = int(os.getenv('TELEGRAM_API_ID'))
TELEGRAM_API_HASH = os.getenv('TELEGRAM_API_HASH')
TELEGRAM_PHONE = os.getenv('TELEGRAM_PHONE')
SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')

SHEET_CONTACTS = "Контакты"
SHEET_STATS = "Статистика"
SHEET_LOG = "Лог"

MAX_GROUPS_PER_RUN = 10
DELAY_MIN = 2
DELAY_MAX = 5

DEFAULT_CRITERIA = {
    'max_contacts': 100,
    'priority': 'any',
    'exclude_bots': False,
}

logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
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

class GoogleSheetsManager:
    def __init__(self):
        self.client = None
        self.spreadsheet = None
        self.connected = False
    
    def connect(self):
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(SPREADSHEET_ID)
            self.connected = True
            logger.info("✅ Connected to Google Sheets")
            print("✅ Connected to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"❌ Google Sheets error: {e}")
            print(f"❌ Google Sheets error: {e}")
            return False
    
    def write_contacts(self, contacts: List[Dict]):
        if not contacts:
            return
        try:
            sheet = self.spreadsheet.worksheet(SHEET_CONTACTS)
            rows = []
            for contact in contacts:
                row = [
                    contact.get('id', ''),
                    contact.get('username', ''),
                    contact.get('phone', ''),
                    contact.get('first_name', ''),
                    contact.get('last_name', ''),
                    contact.get('group', ''),
                    0,
                    '',
                    '',
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                rows.append(row)
            sheet.append_rows(rows)
            logger.info(f"✅ Saved {len(contacts)} contacts")
            print(f"✅ Saved {len(contacts)} contacts")
        except Exception as e:
            logger.error(f"Error saving contacts: {e}")
            print(f"Error saving contacts: {e}")
    
    def write_stats(self, stats: Dict):
        try:
            sheet = self.spreadsheet.worksheet(SHEET_STATS)
            row = [
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                stats.get('groups_parsed', 0),
                stats.get('total_contacts', 0),
                stats.get('with_username', 0),
                stats.get('with_phone', 0),
                stats.get('duration_sec', 0),
                stats.get('errors', 0)
            ]
            sheet.append_row(row)
        except Exception as e:
            logger.error(f"Error saving stats: {e}")
    
    def log(self, message: str, level: str = "INFO"):
        try:
            sheet = self.spreadsheet.worksheet(SHEET_LOG)
            emoji = {'INFO': '✅', 'WARN': '⚠️', 'ERROR': '❌'}
            row = [datetime.now().strftime('%Y-%m-%d %H:%M:%S'), f"{emoji.get(level, '')} {level}", "Bot", message, ""]
            sheet.append_row(row)
        except:
            pass

sheets_manager = GoogleSheetsManager()

class TelegramParser:
    def __init__(self):
        self.client = None
    
    async def connect(self):
        try:
            self.client = TelegramClient('bot_session', TELEGRAM_API_ID, TELEGRAM_API_HASH)
            await self.client.start(phone=TELEGRAM_PHONE)
            logger.info("✅ Connected to Telegram")
            print("✅ Connected to Telegram")
            return True
        except Exception as e:
            logger.error(f"❌ Telegram error: {e}")
            print(f"❌ Telegram error: {e}")
            return False
    
    async def parse_group(self, group_link: str, max_contacts: int, priority: str, exclude_bots: bool) -> List[Dict]:
        contacts = []
        try:
            print(f"🔍 Получаю информацию о {group_link}")
            entity = await self.client.get_entity(group_link)
            
            if hasattr(entity, 'broadcast') and entity.broadcast:
                print(f"📢 Это канал! Ищу группу обсуждений...")
                try:
                    from telethon.tl.functions.channels import GetFullChannelRequest
                    full = await self.client(GetFullChannelRequest(channel=entity))
                    
                    if full.full_chat.linked_chat_id:
                        print(f"✅ Найдена группа обсуждений! ID: {full.full_chat.linked_chat_id}")
                        discussion_group = await self.client.get_entity(full.full_chat.linked_chat_id)
                        entity = discussion_group
                    else:
                        print(f"❌ У канала НЕТ группы обсуждений")
                        return []
                except Exception as e:
                    print(f"❌ Ошибка получения группы обсуждений: {e}")
                    return []
            else:
                print(f"👥 Это группа (не канал)")
            
            print(f"📊 Получаю участников (limit={max_contacts * 2})...")
            participants = await self.client.get_participants(entity, limit=max_contacts * 2)
            print(f"👥 Получено {len(participants)} участников")
            
            for user in participants:
                if len(contacts) >= max_contacts:
                    break
                if exclude_bots and user.bot:
                    continue
                if user.deleted:
                    continue
                if priority == 'username' and not user.username:
                    continue
                
                contact = {
                    'id': user.id,
                    'username': f"@{user.username}" if user.username else "",
                    'phone': f"+{user.phone}" if user.phone else "",
                    'first_name': user.first_name or "",
                    'last_name': user.last_name or "",
                    'group': group_link,
                }
                contacts.append(contact)
                await asyncio.sleep(0.1)
            
            print(f"✅ Отобрано {len(contacts)} контактов после фильтров")
            
        except Exception as e:
            print(f"❌ Ошибка парсинга {group_link}: {e}")
            logger.error(f"Error parsing {group_link}: {e}")
        
        return contacts
    
    async def disconnect(self):
        if self.client:
            await self.client.disconnect()

parser = TelegramParser()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = """👋 Привет!

Я бот для парсинга Telegram групп.

Команда: /parse @groupname

Пример: /parse @durov"""
    await update.message.reply_text(welcome_text)

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
    text = f"""📋 Настройки:

📊 Контактов: {criteria['max_contacts']}
🎯 Приоритет: {criteria['priority']}
🤖 Боты: {'исключены' if criteria['exclude_bots'] else 'включены'}

Группы: {', '.join(groups)}"""
    
    keyboard = [
        [InlineKeyboardButton(f"📊 {criteria['max_contacts']}", callback_data="adjust_max")],
        [InlineKeyboardButton(f"🤖 Боты: {'OFF' if criteria['exclude_bots'] else 'ON'}", callback_data="toggle_bots")],
        [InlineKeyboardButton("🚀 ПАРСИТЬ!", callback_data=f"start:{','.join(groups)}")],
    ]
    
    await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id
    data = query.data
    
    if data.startswith("start:"):
        groups_str = data.split(":", 1)[1]
        groups = [g.strip() for g in groups_str.split(',')]
        await start_parsing(query, user_id, groups)
    elif data == "adjust_max":
        criteria = get_user_criteria(user_id)
        new_value = 50 if criteria['max_contacts'] >= 200 else criteria['max_contacts'] + 50
        update_user_criteria(user_id, 'max_contacts', new_value)
        await update_criteria_msg(query, user_id)
    elif data == "toggle_bots":
        criteria = get_user_criteria(user_id)
        update_user_criteria(user_id, 'exclude_bots', not criteria['exclude_bots'])
        await update_criteria_msg(query, user_id)

async def update_criteria_msg(query, user_id: int):
    criteria = get_user_criteria(user_id)
    text = query.message.text
    groups_line = [line for line in text.split('\n') if 'Группы:' in line]
    groups_str = groups_line[0].split(':', 1)[1].strip() if groups_line else ""
    
    new_text = f"""📋 Настройки:

📊 Контактов: {criteria['max_contacts']}
🎯 Приоритет: {criteria['priority']}
🤖 Боты: {'исключены' if criteria['exclude_bots'] else 'включены'}

Группы: {groups_str}"""
    
    keyboard = [
        [InlineKeyboardButton(f"📊 {criteria['max_contacts']}", callback_data="adjust_max")],
        [InlineKeyboardButton(f"🤖 Боты: {'OFF' if criteria['exclude_bots'] else 'ON'}", callback_data="toggle_bots")],
        [InlineKeyboardButton("🚀 ПАРСИТЬ!", callback_data=f"start:{groups_str}")],
    ]
    
    await query.edit_message_text(new_text, reply_markup=InlineKeyboardMarkup(keyboard))

async def start_parsing(query, user_id: int, groups: List[str]):
    print(f"▶️ START_PARSING вызвана для групп: {groups}")
    await query.edit_message_text("🚀 Начинаю...\n⏳ Подключаюсь...")
    
    try:
        print(f"📡 Проверяю подключение к Telegram...")
        if not parser.client or not parser.client.is_connected():
            print(f"⚠️ Клиент не подключен, подключаюсь...")
            if not await parser.connect():
                await query.edit_message_text("❌ Ошибка подключения!")
                return
        
        criteria = get_user_criteria(user_id)
        all_contacts = []
        start_time = time.time()
        
        for idx, group in enumerate(groups, 1):
            await query.edit_message_text(f"📡 Группа {idx}/{len(groups)}: {group}")
            
            print(f"🎯 Вызываю parse_group для {group}")
            print(f"   Параметры: max={criteria['max_contacts']}, priority={criteria['priority']}, exclude_bots={criteria['exclude_bots']}")
            
            contacts = await parser.parse_group(group, criteria['max_contacts'], criteria['priority'], criteria['exclude_bots'])
            
            print(f"📦 parse_group вернул {len(contacts)} контактов")
            
            all_contacts.extend(contacts)
            
            await query.edit_message_text(f"✅ {group}: {len(contacts)} контактов\n📊 Всего: {len(all_contacts)}")
            await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        
        if all_contacts:
            await query.edit_message_text("💾 Сохраняю...")
            sheets_manager.write_contacts(all_contacts)
            
            stats = {
                'groups_parsed': len(groups),
                'total_contacts': len(all_contacts),
                'with_username': sum(1 for c in all_contacts if c.get('username')),
                'with_phone': sum(1 for c in all_contacts if c.get('phone')),
                'duration_sec': int(time.time() - start_time),
                'errors': 0
            }
            sheets_manager.write_stats(stats)
        
        result = f"""✅ Готово!

📊 Результаты:
- Групп: {len(groups)}
- Контактов: {len(all_contacts)}
- Username: {sum(1 for c in all_contacts if c.get('username'))}
- Телефон: {sum(1 for c in all_contacts if c.get('phone'))}

📋 Данные в Google Sheets!"""
        
        await query.edit_message_text(result)
        
    except Exception as e:
        logger.error(f"Parsing error: {e}")
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        await query.edit_message_text(f"❌ Ошибка: {str(e)}")

def main():
    import subprocess
    subprocess.run(['python', 'decode_session.py'])
    
    print("🔗 Connecting to Google Sheets...")
    if not sheets_manager.connect():
        print("❌ Google Sheets connection failed!")
        return
    
    print("🤖 Starting bot...")
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("parse", parse_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    
    print("✅ Bot started!")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
