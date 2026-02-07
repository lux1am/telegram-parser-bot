#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
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
            logger.info("✅ Connected to Google Sheets")
            return True
        except Exception as e:
            logger.error(f"❌ Google Sheets error: {e}")
            return False
    
    def write_contacts(self, contacts: List[Dict]):
        if not contacts:
            logger.warning("⚠️ No contacts to write")
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
            logger.info(f"✅ Saved {len(contacts)} contacts to Google Sheets")
        except Exception as e:
            logger.error(f"❌ Error saving contacts: {e}")
    
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
        except Exception as e:
            logger.error(f"Error saving stats: {e}")

sheets_manager = GoogleSheetsManager()

class TelegramParser:
    def __init__(self):
        self.client = None
        self.loop = None
    
    async def connect(self):
        try:
            logger.info("🔌 Creating Telethon client...")
            if not self.loop:
                self.loop = asyncio.new_event_loop()
            
            self.client = TelegramClient('bot_session', TELEGRAM_API_ID, TELEGRAM_API_HASH, loop=self.loop)
            logger.info("🔑 Starting Telethon authentication...")
            await self.client.start(phone=TELEGRAM_PHONE)
            logger.info("✅ Connected to Telegram via Telethon")
            return True
        except Exception as e:
            logger.error(f"❌ Telegram connection error: {e}", exc_info=True)
            return False
    
    async def parse_group(self, group_link: str, max_contacts: int) -> List[Dict]:
        contacts = []
        logger.info("="*60)
        logger.info(f"🎯 PARSING GROUP: {group_link}")
        logger.info(f"📊 Max contacts limit: {max_contacts}")
        
        try:
            logger.info(f"📡 Getting entity for {group_link}...")
            entity = await self.client.get_entity(group_link)
            entity_title = entity.title if hasattr(entity, 'title') else 'Unnamed'
            logger.info(f"✅ Got entity: {entity_title}")
            logger.info(f"   Type: {'Channel' if hasattr(entity, 'broadcast') and entity.broadcast else 'Group'}")
            
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
            
            logger.info("📥 Requesting ALL participants from Telegram...")
            participants = await self.client.get_participants(entity)
            logger.info(f"✅ Telegram returned {len(participants)} total participants")
            
            if len(participants) > max_contacts:
                logger.info(f"✂️ Limiting to first {max_contacts} participants")
                participants = participants[:max_contacts]
            
            logger.info(f"🔄 Processing {len(participants)} participants...")
            
            bots_count = 0
            deleted_count = 0
            
            for idx, user in enumerate(participants, 1):
                if user.deleted:
                    deleted_count += 1
                    continue
                
                if user.bot:
                    bots_count += 1
                
                contact = {
                    'id': user.id,
                    'username': f"@{user.username}" if user.username else "",
                    'phone': f"+{user.phone}" if user.phone else "",
                    'first_name': user.first_name or "",
                    'last_name': user.last_name or "",
                    'group': group_link,
                }
                contacts.append(contact)
                
                if idx % 100 == 0:
                    logger.info(f"   📦 Processed {idx}/{len(participants)} participants...")
                
                await asyncio.sleep(0.05)
            
            logger.info("="*60)
            logger.info(f"✅ PARSING COMPLETE!")
            logger.info(f"   📊 Total collected: {len(contacts)} contacts")
            logger.info(f"   🤖 Bots found: {bots_count}")
            logger.info(f"   🗑️ Deleted accounts skipped: {deleted_count}")
            logger.info(f"   👤 With username: {sum(1 for c in contacts if c['username'])}")
            logger.info(f"   📱 With phone: {sum(1 for c in contacts if c['phone'])}")
            logger.info("="*60)
            
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
        new_val = 1000 if criteria['max_contacts'] >= 10000 else criteria['max_contacts'] + 1000
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
    logger.info("#"*60)
    logger.info("🚀 PARSING SESSION STARTED")
    logger.info(f"👤 User ID: {user_id}")
    logger.info(f"📝 Groups to parse: {len(groups)}")
    logger.info(f"📋 Groups: {', '.join(groups)}")
    logger.info("#"*60)
    
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
            logger.info(f"\n{'~'*60}")
            logger.info(f"📡 Parsing group {idx}/{len(groups)}: {group}")
            logger.info(f"{'~'*60}")
            
            await query.edit_message_text(f"📡 Парсинг {idx}/{len(groups)}: {group}...")
            
            contacts = await parser.parse_group(group, criteria['max_contacts'])
            all_contacts.extend(contacts)
            
            logger.info(f"➕ Added {len(contacts)} contacts from {group}")
            logger.info(f"📊 Total contacts so far: {len(all_contacts)}")
            
            await query.edit_message_text(
                f"✅ {group}: {len(contacts)} контактов\n📊 Всего: {len(all_contacts)}"
            )
            
            if idx < len(groups):
                delay = random.uniform(DELAY_MIN, DELAY_MAX)
                logger.info(f"⏳ Waiting {delay:.1f} seconds before next group...")
                await asyncio.sleep(delay)
        
        if all_contacts:
            logger.info("💾 Saving contacts to Google Sheets...")
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
        else:
            logger.warning("⚠️ No contacts collected!")
        
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
        
        logger.info("#"*60)
        logger.info("✅ PARSING SESSION COMPLETED SUCCESSFULLY")
        logger.info(f"📊 Total contacts collected: {len(all_contacts)}")
        logger.info(f"⏱️ Duration: {int(time.time() - start_time)} seconds")
        logger.info("#"*60)
        
    except Exception as e:
        logger.error("!"*60)
        logger.error("❌ CRITICAL ERROR in do_parsing")
        logger.error(f"Error type: {type(e).__name__}")
        logger.error(f"Error message: {str(e)}", exc_info=True)
        logger.error("!"*60)
        await query.edit_message_text(f"❌ Ошибка: {str(e)}")

def main():
    import subprocess
    subprocess.run(['python', 'decode_session.py'], check=False)
    
    logger.info("="*60)
    logger.info("🔗 Connecting to Google Sheets...")
    if not sheets_manager.connect():
        logger.error("❌ Failed to connect to Google Sheets!")
        return
    
    logger.info("🤖 Starting Telegram bot...")
    app = Application.builder().token(BOT_TOKEN).build()
    
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("parse", parse_command))
    app.add_handler(CallbackQueryHandler(button_callback))
    
    logger.info("✅ Bot is running and ready!")
    logger.info("="*60)
    
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
