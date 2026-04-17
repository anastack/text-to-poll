from __future__ import annotations

import asyncio
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from .config import load_config
from .parser import ParseError, parse_quiz_text
from .state import ChannelSelectionStore, PhotoCache


logger = logging.getLogger(__name__)


def _is_allowed(m: Message, admin_user_id: int | None) -> bool:
    if admin_user_id is None:
        return True
    return bool(m.from_user and m.from_user.id == admin_user_id)


def _normalize_channel_id(text: str) -> str | None:
    channel_id = text.strip()
    if not channel_id:
        return None
    if channel_id.startswith("@"):
        return channel_id
    if channel_id.startswith("-") and channel_id[1:].isdigit():
        return channel_id
    return None


async def _ask_channel_id(m: Message) -> None:
    await m.answer(
        "Пришлите ID канала, куда публиковать викторины.\n\n"
        "Примеры:\n"
        "-1001234567890\n"
        "@your_public_channel\n\n"
        "Бот должен быть админом этого канала."
    )


async def _post_quiz(
    *,
    bot: Bot,
    channel_id: str,
    question_text: str,
    photo_file_id: str | None,
    poll_anonymous: bool,
    poll_multiple_answers: bool,
) -> str:
    parsed = parse_quiz_text(question_text)

    if photo_file_id:
        await bot.send_photo(chat_id=channel_id, photo=photo_file_id)

    # Telegram limitation: non-anonymous polls can't be sent to channels.
    if channel_id.startswith("@") or channel_id.startswith("-100"):
        poll_anonymous = True

    if parsed.has_multiple_correct_options:
        await bot.send_poll(
            chat_id=channel_id,
            question=parsed.question,
            options=parsed.options,
            type="regular",
            is_anonymous=poll_anonymous,
            allows_multiple_answers=True,
        )
        return f"Опубликовал опрос с несколькими вариантами ответа в канал {channel_id}."

    await bot.send_poll(
        chat_id=channel_id,
        question=parsed.question,
        options=parsed.options,
        type="quiz",
        correct_option_id=parsed.correct_option_id,
        is_anonymous=poll_anonymous,
        allows_multiple_answers=poll_multiple_answers,
    )
    return f"Опубликовал викторину в канал {channel_id}."


def build_router(*, photo_cache: PhotoCache, channel_store: ChannelSelectionStore, cfg) -> Router:
    router = Router()
    users_waiting_for_channel: set[int] = set()

    async def ensure_channel(m: Message) -> str | None:
        if not m.from_user:
            return None

        channel_id = channel_store.get(m.from_user.id)
        if channel_id:
            return channel_id

        users_waiting_for_channel.add(m.from_user.id)
        await _ask_channel_id(m)
        return None

    @router.message(CommandStart())
    async def start(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return

        if not m.from_user:
            return

        channel_id = channel_store.get(m.from_user.id)
        if not channel_id:
            users_waiting_for_channel.add(m.from_user.id)
            await _ask_channel_id(m)
            return

        await m.answer(
            f"Текущий канал: {channel_id}\n\n"
            "Пришлите вопрос и варианты ответов, каждый с новой строки.\n"
            "Правильный вариант пометьте * или +.\n\n"
            "Чтобы заменить канал, отправьте /channel."
        )

    @router.message(Command("channel"))
    async def channel_cmd(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return
        if not m.from_user:
            return

        users_waiting_for_channel.add(m.from_user.id)
        await _ask_channel_id(m)

    @router.message(Command("help"))
    async def help_cmd(m: Message) -> None:
        await m.answer(
            "Формат викторины:\n"
            "1-я строка - вопрос\n"
            "дальше - варианты ответа (2-10)\n"
            "правильный вариант: * или + в начале строки\n\n"
            "Пример:\n"
            "Сколько будет 2+2?\n"
            "*4\n"
            "3\n"
            "5\n\n"
            "Канал меняется командой /channel."
        )

    @router.message(F.photo)
    async def on_photo(m: Message, bot: Bot) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return

        if not m.from_user:
            return

        channel_id = await ensure_channel(m)
        if not channel_id:
            return

        photo = m.photo[-1]
        caption = (m.caption or "").strip()

        if caption:
            try:
                msg = await _post_quiz(
                    bot=bot,
                    channel_id=channel_id,
                    question_text=caption,
                    photo_file_id=photo.file_id,
                    poll_anonymous=cfg.poll_anonymous,
                    poll_multiple_answers=cfg.poll_multiple_answers,
                )
            except ParseError as e:
                await m.answer(f"Не смог распарсить подпись: {e}")
                return
            await m.answer(msg)
            return

        photo_cache.set(m.from_user.id, photo.file_id)
        await m.answer(
            "Ок. Теперь пришлите текст вопроса (вопрос + варианты), прикреплю это фото."
        )

    @router.message(F.text)
    async def on_text(m: Message, bot: Bot) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return

        if not m.from_user:
            return

        text = (m.text or "").strip()
        if text.startswith("/"):
            return

        if m.from_user.id in users_waiting_for_channel:
            channel_id = _normalize_channel_id(text)
            if not channel_id:
                await m.answer(
                    "Не похоже на ID канала. Пришлите ID вида -1001234567890 "
                    "или username публичного канала вида @your_public_channel."
                )
                return

            channel_store.set(m.from_user.id, channel_id)
            users_waiting_for_channel.discard(m.from_user.id)
            await m.answer(
                f"Канал сохранен: {channel_id}\n"
                "Теперь он используется по умолчанию. Чтобы заменить канал, отправьте /channel."
            )
            return

        channel_id = await ensure_channel(m)
        if not channel_id:
            return

        photo_file_id = photo_cache.pop_if_fresh(m.from_user.id)
        try:
            msg = await _post_quiz(
                bot=bot,
                channel_id=channel_id,
                question_text=text,
                photo_file_id=photo_file_id,
                poll_anonymous=cfg.poll_anonymous,
                poll_multiple_answers=cfg.poll_multiple_answers,
            )
        except ParseError as e:
            await m.answer(f"Не смог распарсить текст: {e}")
            return
        await m.answer(msg)

    return router


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    cfg = load_config()
    bot = Bot(token=cfg.bot_token)
    dp = Dispatcher()

    photo_cache = PhotoCache(ttl_seconds=cfg.photo_ttl_seconds)
    channel_store = ChannelSelectionStore(
        Path(__file__).resolve().parent.parent / "channel_selections.json"
    )
    dp.include_router(
        build_router(photo_cache=photo_cache, channel_store=channel_store, cfg=cfg)
    )

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
