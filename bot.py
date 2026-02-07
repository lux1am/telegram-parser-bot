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

SHEET_CONTACTS = "Контакты"
SHEET_STATS = "Статистика"

MAX_GROUPS_PER_RUN = 10
DELAY_MIN = 2
DELAY_MAX = 5

DEFAULT_CRITERIA = {
    'max_contacts': 10000,
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
    
    def connect(self):
        try:
            scopes = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
            creds = Credentials.from_service_account_file('credentials.json', scopes=scopes)
            self.client = gspread.authorize(creds)
            self.spreadsheet = self.client.open_by_key(SPREADSHEET_ID)
            print("✅ Connected to Google Sheets")
            return True
        except Exception as e:
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
                    0, '', '',
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ]
                rows.append(row)
            sheet.append_rows(rows)
            print(f"✅ Saved {len(contacts)} contacts to Sheets")
        except Exception as e:
            print(f"❌ Error saving contacts: {e}")
    
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
                0
            ]
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
            print("✅ Connected to Telegram")
            return True
        except Exception as e:
            print(f"❌ Telegram connection error: {e}")
            return False
    
    async def parse_group(self, group_link: str, max_contacts: int) -> List[Dict]:
        contacts = []
        print(f"\n{'='*50}")
        print(f"🎯 Парсинг: {group_link}")
        print(f"📊 Лимит: {max_contacts} контактов")
        
        try:
            entity = await self.client.get_entity(group_link)
            print(f"✅ Получена сущность: {entity.title if hasattr(entity, 'title') else 'Без названия'}")
            
            if hasattr(entity, 'broadcast') and entity.broadcast:
                print(f"📢 Обнаружен КАНАЛ, ищу группу обсуждений...")
                try:
                    full = await self.client(GetFullChannelRequest(channel=entity))
                    if full.full_chat.linked_chat_id:
                        print(f"✅ Найдена группа обсуждений (ID: {full.full_chat.linked_chat_id})")
                        entity = await self.client.get_entity(full.full_chat.linked_chat_id)
                        print(f"✅ Переключились на группу: {entity.title if hasattr(entity, 'title') else ''}")
                    else:
                        print(f"❌ У канала НЕТ привязанной группы обсуждений")
                        return []
                except Exception as e:
                    print(f"❌ Ошибка доступа к группе обсуждений: {e}")
                    return []
            else:
                print(f"👥 Обнаружена ГРУППА")
            
            print(f"📥 Запрашиваю участников (limit={max_contacts})...")
           participants = await self.client.get_participants(entity)
            print(f"✅ Telegram вернул {len(participants)} участников")
            
            for idx, user in enumerate(participants, 1):
                if user.deleted:
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
                
                if idx % 50 == 0:
                    print(f"   📦 Обработано {idx}/{len(participants)}...")
                
                await asyncio.sleep(0.05)
            
            print(f"✅ ИТОГО собрано: {len(contacts)} контактов")
            print(f"   • С username: {sum(1 for c in contacts if c['username'])}")
            print(f"   • С телефоном: {sum(1 for c in contacts if c['phone'])}")
            print(f"{'='*50}\n")
            
        except Exception as e:
            print(f"❌ КРИТИЧЕСКАЯ ОШИБКА при парсинге {group_link}:")
            print(f"   {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
        
        return contacts
    
    async def disconnect(self):
        if self.client:
            await self.client.disconnect()

parser = TelegramParser()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 Привет! Я парсер Telegram групп.\n\n"
        "Команда: /parse @groupname\n"
        "Пример: /parse @python"
    )

async def parse_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    
    if not context.args:
        await update.message.reply_text("❌ Укажи группу!\n\nПример: /parse @groupname")
        return
    
    groups_str = ' '.join(context.args)
    groups = [g.strip() for g in groups_str.replace(',', ' ').split() if g.strip()]
    
    if len(groups) > MAX_GROUPS_PER_RUN:
        await update.message.reply_text(f"⚠️ Максимум {MAX_GROUPS_PER_RUN} групп!")
        return
    
    criteria = get_user_criteria(user_id)
    
    keyboard = [
        [InlineKeyboardButton(f"📊 Контактов: {criteria['max_contacts']}", callback_data="adj")],
        [InlineKeyboardButton("🚀 ПАРСИТЬ!", callback_data=f"go:{','.join(groups)}")],
    ]
    
    await update.message.reply_text(
        f"📋 Настройки:\n📊 Макс. контактов: {criteria['max_contacts']}\n\nГруппы: {', '.join(groups)}",
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
        new_val = 50 if criteria['max_contacts'] >= 200 else criteria['max_contacts'] + 50
        update_user_criteria(user_id, 'max_contacts', new_val)
        
        text = query.message.text
        groups_line = [l for l in text.split('\n') if 'Группы:' in l]
        groups_str = groups_line[0].split(':', 1)[1].strip() if groups_line else ""
        
        keyboard = [
            [InlineKeyboardButton(f"📊 Контактов: {new_val}", callback_data="adj")],
            [InlineKeyboardButton("🚀 ПАРСИТЬ!", callback_data=f"go:{groups_str}")],
        ]
        
        await query.edit_message_text(
            f"📋 Настройки:\n📊 Макс. контактов: {new_val}\n\nГруппы: {groups_str}",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def do_parsing(query, user_id: int, groups: List[str]):
    print(f"\n{'#'*60}")
    print(f"🚀 НАЧАЛО ПАРСИНГА")
    print(f"👤 User ID: {user_id}")
    print(f"📝 Групп: {len(groups)}")
    print(f"{'#'*60}\n")
    
    await query.edit_message_text("🚀 Подключаюсь к Telegram...")
    
    try:
        if not parser.client or not parser.client.is_connected():
            if not await parser.connect():
                await query.edit_message_text("❌ Ошибка подключения к Telegram!")
                return
        
        criteria = get_user_criteria(user_id)
        all_contacts = []
        start_time = time.time()
        
        for idx, group in enumerate(groups, 1):
            await query.edit_message_text(f"📡 Парсинг {idx}/{len(groups)}: {group}...")
            
            contacts = await parser.parse_group(group, criteria['max_contacts'])
            all_contacts.extend(contacts)
            
            await query.edit_message_text(
                f"✅ {group}: {len(contacts)} контактов\n📊 Всего: {len(all_contacts)}"
            )
            
            if idx < len(groups):
                await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))
        
        if all_contacts:
            await query.edit_message_text("💾 Сохраняю в Google Sheets...")
            sheets_manager.write_contacts(all_contacts)
            
            stats = {
                'groups_parsed': len(groups),
                'total_contacts': len(all_contacts),
                'with_username': sum(1 for c in all_contacts if c.get('username')),
                'with_phone': sum(1 for c in all_contacts if c.get('phone')),
                'duration_sec': int(time.time() - start_time),
            }
            sheets_manager.write_stats(stats)
        
        result = (
            f"✅ Парсинг завершён!\n\n"
            f"📊 Результаты:\n"
            f"• Групп: {len(groups)}\n"
            f"• Контактов: {len(all_contacts)}\n"
            f"• С username: {sum(1 for c in all_contacts if c.get('username'))}\n"
            f"• С телефоном: {sum(1 for c in all_contacts if c.get('phone'))}\n\n"
            f"📋 Данные в Google Sheets!"
        )
        
        await query.edit_message_text(result)
        
        print(f"\n{'#'*60}")
        print(f"✅ ПАРСИНГ ЗАВЕРШЁН УСПЕШНО")
        print(f"📊 Собрано: {len(all_contacts)} контактов")
        print(f"{'#'*60}\n")
        
    except Exception as e:
        print(f"\n{'!'*60}")
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА В do_parsing:")
        print(f"   {type(e).__name__}: {e}")
        print(f"{'!'*60}\n")
        import traceback
        traceback.print_exc()
        await query.edit_message_text(f"❌ Ошибка: {str(e)}")

def main():
    import subprocess
    subprocess.run(['python', 'decode_session.py'], check=False)
    
    print("\n" + "="*60)
    print("🔗 Connecting to Google Sheets...")
    if not sheets_manager.connect():
        print("❌ Failed to connect to Google Sheets!")
        return
    
    print("🤖 Starting Telegram bot...")
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("parse", parse_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    
    print("✅ Bot is running!")
    print("="*60 + "\n")
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
