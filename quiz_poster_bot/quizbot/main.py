from __future__ import annotations

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import re
import time

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import Message

from .config import load_config
from .parser import ParseError, parse_quiz_block_text, parse_quiz_text
from .state import ChannelSelectionStore, PhotoCache, ScheduledQuizJob, ScheduledQuizStore


logger = logging.getLogger(__name__)
_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhdсмчд]?)\s*$", re.IGNORECASE)


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


def _parse_delay(raw: str) -> int | None:
    match = _DURATION_RE.match(raw)
    if not match:
        return None

    value = int(match.group(1))
    unit = match.group(2).lower()
    if value <= 0:
        return None
    if unit in {"", "m", "м"}:
        return value * 60
    if unit in {"s", "с"}:
        return value
    if unit in {"h", "ч"}:
        return value * 60 * 60
    if unit in {"d", "д"}:
        return value * 24 * 60 * 60
    return None


def _format_when(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp).strftime("%d.%m.%Y %H:%M:%S")


def _split_command_payload(text: str) -> tuple[str, str]:
    lines = text.replace("\r\n", "\n").split("\n", 1)
    header = lines[0].strip()
    body = lines[1].strip() if len(lines) > 1 else ""
    return header, body


def _command_tail(header: str) -> str:
    parts = header.split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def _looks_like_block(text: str) -> bool:
    normalized = text.replace("\r\n", "\n")
    return bool(
        re.search(r"(?mi)^\s*(?:тема|topic)\s*:", normalized)
        or re.search(r"(?m)^\s*#", normalized)
        or re.search(r"(?m)^\s*---+\s*$", normalized)
        or re.search(r"\n\s*\n", normalized)
    )


async def _ask_channel_id(m: Message) -> None:
    await m.answer(
        "Пришлите ID канала, куда публиковать викторины.\n\n"
        "Примеры:\n"
        "-1001234567890\n"
        "@your_public_channel\n\n"
        "Бот должен быть админом этого канала."
    )


async def _post_one_quiz(
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


async def _post_quiz_block(
    *,
    bot: Bot,
    channel_id: str,
    question_text: str,
    topic: str | None,
    photo_file_id: str | None,
    poll_anonymous: bool,
    poll_multiple_answers: bool,
) -> str:
    block = parse_quiz_block_text(question_text, topic=topic)

    if photo_file_id:
        await bot.send_photo(chat_id=channel_id, photo=photo_file_id)

    if block.topic:
        await bot.send_message(chat_id=channel_id, text=f"Тема: {block.topic}")

    for parsed in block.quizzes:
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
        else:
            await bot.send_poll(
                chat_id=channel_id,
                question=parsed.question,
                options=parsed.options,
                type="quiz",
                correct_option_id=parsed.correct_option_id,
                is_anonymous=poll_anonymous,
                allows_multiple_answers=poll_multiple_answers,
            )
        await asyncio.sleep(0.4)

    word = "вопрос" if len(block.quizzes) == 1 else "вопроса" if 2 <= len(block.quizzes) <= 4 else "вопросов"
    topic_suffix = f" по теме «{block.topic}»" if block.topic else ""
    return f"Опубликовал блок: {len(block.quizzes)} {word}{topic_suffix} в канал {channel_id}."


async def _run_scheduled_job(
    *,
    bot: Bot,
    job: ScheduledQuizJob,
    schedule_store: ScheduledQuizStore,
    cfg,
) -> None:
    delay = max(0, job.send_at - time.time())
    if delay:
        await asyncio.sleep(delay)

    try:
        await _post_quiz_block(
            bot=bot,
            channel_id=job.channel_id,
            question_text=job.question_text,
            topic=job.topic,
            photo_file_id=job.photo_file_id,
            poll_anonymous=cfg.poll_anonymous,
            poll_multiple_answers=cfg.poll_multiple_answers,
        )
    except Exception:
        logger.exception("Scheduled quiz job %s failed", job.id)
        return

    schedule_store.remove(job.id)


def _schedule_job_task(
    *,
    bot: Bot,
    job: ScheduledQuizJob,
    schedule_store: ScheduledQuizStore,
    cfg,
) -> None:
    asyncio.create_task(
        _run_scheduled_job(bot=bot, job=job, schedule_store=schedule_store, cfg=cfg)
    )


def build_router(
    *,
    bot: Bot,
    photo_cache: PhotoCache,
    channel_store: ChannelSelectionStore,
    schedule_store: ScheduledQuizStore,
    cfg,
) -> Router:
    router = Router()
    users_waiting_for_channel: set[int] = set()

    async def ensure_channel(m: Message) -> str | None:
        if not m.from_user:
            return None

        channel_id = channel_store.get(m.from_user.id) or cfg.target_channel_id
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

        channel_id = channel_store.get(m.from_user.id) or cfg.target_channel_id
        if not channel_id:
            users_waiting_for_channel.add(m.from_user.id)
            await _ask_channel_id(m)
            return

        await m.answer(
            f"Текущий канал: {channel_id}\n\n"
            "Один вопрос можно отправить обычным сообщением: вопрос и варианты с новой строки, правильный вариант пометьте * или +.\n\n"
            "Блок вопросов:\n"
            "/block История\n"
            "Тема: История\n"
            "Вопрос 1?\n"
            "*Верный ответ\n"
            "Ответ 2\n\n"
            "Вопрос 2?\n"
            "*Верный ответ\n"
            "Ответ 2\n\n"
            "Отложить блок: /schedule 10m История\n"
            "Единицы времени: 30s, 10m, 2h, 1d. Канал меняется командой /channel."
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
            "Формат одного вопроса:\n"
            "1-я строка - вопрос\n"
            "дальше - варианты ответа (2-10)\n"
            "правильный вариант: * или + в начале строки\n\n"
            "Блок вопросов:\n"
            "/block Тема\n"
            "Вопрос 1?\n"
            "*Ответ\n"
            "Другой ответ\n\n"
            "Вопрос 2?\n"
            "*Ответ\n"
            "Другой ответ\n\n"
            "Отложенная отправка:\n"
            "/schedule 10m Тема\n"
            "и дальше такой же блок вопросов.\n\n"
            "Вопросы можно разделять пустой строкой или строкой ---."
        )

    @router.message(Command("scheduled"))
    async def scheduled_cmd(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return

        jobs = schedule_store.list_all()
        if not jobs:
            await m.answer("Отложенных блоков пока нет.")
            return

        lines = ["Отложенные блоки:"]
        for job in jobs:
            topic = f" - {job.topic}" if job.topic else ""
            lines.append(f"{job.id[:8]}: {_format_when(job.send_at)}{topic} -> {job.channel_id}")
        await m.answer("\n".join(lines))

    @router.message(Command("block"))
    async def block_cmd(m: Message, bot: Bot) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return
        if not m.from_user:
            return

        channel_id = await ensure_channel(m)
        if not channel_id:
            return

        header, body = _split_command_payload(m.text or "")
        topic = _command_tail(header) or None
        if not body:
            await m.answer("После /block добавьте блок вопросов. Например: /block История, а ниже вопросы.")
            return

        photo_file_id = photo_cache.pop_if_fresh(m.from_user.id)
        try:
            msg = await _post_quiz_block(
                bot=bot,
                channel_id=channel_id,
                question_text=body,
                topic=topic,
                photo_file_id=photo_file_id,
                poll_anonymous=cfg.poll_anonymous,
                poll_multiple_answers=cfg.poll_multiple_answers,
            )
        except ParseError as e:
            await m.answer(f"Не смог разобрать блок: {e}")
            return
        await m.answer(msg)

    @router.message(Command("schedule"))
    async def schedule_cmd(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return
        if not m.from_user:
            return

        channel_id = await ensure_channel(m)
        if not channel_id:
            return

        header, body = _split_command_payload(m.text or "")
        args = _command_tail(header)
        if not args or not body:
            await m.answer(
                "Формат: /schedule 10m Тема\n"
                "Ниже добавьте блок вопросов. Время можно указать как 30s, 10m, 2h или 1d."
            )
            return

        parts = args.split(maxsplit=1)
        delay_seconds = _parse_delay(parts[0])
        if delay_seconds is None:
            await m.answer("Не понял время. Используйте формат 30s, 10m, 2h или 1d.")
            return

        topic = parts[1].strip() if len(parts) > 1 else None
        photo_file_id = photo_cache.pop_if_fresh(m.from_user.id)
        try:
            block = parse_quiz_block_text(body, topic=topic)
        except ParseError as e:
            await m.answer(f"Не смог разобрать блок: {e}")
            return

        job = schedule_store.add(
            user_id=m.from_user.id,
            channel_id=channel_id,
            question_text=body,
            topic=block.topic,
            photo_file_id=photo_file_id,
            send_at=time.time() + delay_seconds,
        )
        _schedule_job_task(bot=bot, job=job, schedule_store=schedule_store, cfg=cfg)

        await m.answer(
            f"Поставил в очередь {len(block.quizzes)} вопросов"
            f"{f' по теме «{block.topic}»' if block.topic else ''}.\n"
            f"Отправка: {_format_when(job.send_at)}.\n"
            f"ID задачи: {job.id[:8]}"
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
                msg = await _post_quiz_block(
                    bot=bot,
                    channel_id=channel_id,
                    question_text=caption,
                    topic=None,
                    photo_file_id=photo.file_id,
                    poll_anonymous=cfg.poll_anonymous,
                    poll_multiple_answers=cfg.poll_multiple_answers,
                )
            except ParseError as e:
                await m.answer(f"Не смог разобрать подпись: {e}")
                return
            await m.answer(msg)
            return

        photo_cache.set(m.from_user.id, photo.file_id)
        await m.answer(
            "Ок. Теперь пришлите текст вопроса или блок вопросов, прикреплю это фото к публикации."
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
            if _looks_like_block(text):
                msg = await _post_quiz_block(
                    bot=bot,
                    channel_id=channel_id,
                    question_text=text,
                    topic=None,
                    photo_file_id=photo_file_id,
                    poll_anonymous=cfg.poll_anonymous,
                    poll_multiple_answers=cfg.poll_multiple_answers,
                )
            else:
                msg = await _post_one_quiz(
                    bot=bot,
                    channel_id=channel_id,
                    question_text=text,
                    photo_file_id=photo_file_id,
                    poll_anonymous=cfg.poll_anonymous,
                    poll_multiple_answers=cfg.poll_multiple_answers,
                )
        except ParseError as e:
            await m.answer(f"Не смог разобрать текст: {e}")
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

    base_path = Path(__file__).resolve().parent.parent
    photo_cache = PhotoCache(ttl_seconds=cfg.photo_ttl_seconds)
    channel_store = ChannelSelectionStore(base_path / "channel_selections.json")
    schedule_store = ScheduledQuizStore(base_path / "scheduled_quizzes.json")

    for job in schedule_store.list_all():
        _schedule_job_task(bot=bot, job=job, schedule_store=schedule_store, cfg=cfg)

    dp.include_router(
        build_router(
            bot=bot,
            photo_cache=photo_cache,
            channel_store=channel_store,
            schedule_store=schedule_store,
            cfg=cfg,
        )
    )

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
