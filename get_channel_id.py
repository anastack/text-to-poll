#!/usr/bin/env python3
"""
Скрипт для получения ID приватного канала.
Запустите этот скрипт и отправьте команду /start боту.
"""

import asyncio
import logging
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command
from aiogram.types import Message
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "quiz_poster_bot", ".env"))

bot_token = os.getenv("BOT_TOKEN")
if not bot_token:
    print("❌ BOT_TOKEN не найден в .env файле!")
    exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()
bot = Bot(token=bot_token)
dp = Dispatcher()


@router.message(Command("start"))
async def cmd_start(message: Message):
    """Обработчик команды /start"""
    user_id = message.from_user.id
    text = f"""
Привет! 🤖

Ваш ID: `{user_id}`

Теперь сделайте следующее:
1. Добавьте этого бота в ваш приватный канал как админа
2. Отправьте боту в ЛС команду /channel
3. Бот покажет ID всех каналов, где он администратор

Команды:
/channel - показать ID каналов
"""
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("channel"))
async def cmd_channel(message: Message):
    """Команда для получения информации о канале"""
    await message.answer("✅ Перед использованием этой команды убедитесь, что:\n\n"
                        "1. Бот добавлен в ваш приватный канал как администратор\n"
                        "2. Вы отправили хотя бы одно сообщение/вопрос в канал при наличии бота там\n\n"
                        "К сожалению, Telegram не предоставляет прямой способ узнать ID канала через бота.\n\n"
                        "🔧 **Альтернативный способ:**\n\n"
                        "Используйте веб-версию Telegram (web.telegram.org):\n"
                        "1. Откройте свой канал\n"
                        "2. Посмотрите URL в адресной строке\n"
                        "3. Цифра в URL → `-100` + эта цифра = ваш channel_id\n\n"
                        "Например: `web.telegram.org/a/c/1234567` → channel_id = `-1001234567`")


if __name__ == "__main__":
    dp.include_router(router)
    asyncio.run(dp.start_polling(bot))
