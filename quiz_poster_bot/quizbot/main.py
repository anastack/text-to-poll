from __future__ import annotations

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import re
import time

from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .config import load_config
from .parser import ParseError, parse_quiz_block_text, parse_quiz_text
from .state import (
    ChannelSelectionStore,
    PhotoCache,
    ScheduledQuizJob,
    ScheduledQuizQuestion,
    ScheduledQuizStore,
)


logger = logging.getLogger(__name__)
_DURATION_RE = re.compile(r"^\s*(\d+)\s*([smhdсмчд]?)\s*$", re.IGNORECASE)


def _is_allowed(m: Message, admin_user_id: int | None) -> bool:
    if admin_user_id is None:
        return True
    return bool(m.from_user and m.from_user.id == admin_user_id)


def _is_callback_allowed(c: CallbackQuery, admin_user_id: int | None) -> bool:
    if admin_user_id is None:
        return True
    return c.from_user.id == admin_user_id


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
    normalized = raw.strip().lower()
    normalized = re.sub(r"^\s*через\s+", "", normalized)
    word_delays = {
        "час": 60 * 60,
        "один час": 60 * 60,
        "два часа": 2 * 60 * 60,
        "три часа": 3 * 60 * 60,
        "полчаса": 30 * 60,
        "завтра": 24 * 60 * 60,
    }
    if normalized in word_delays:
        return word_delays[normalized]

    ru_match = re.match(
        r"^\s*(\d+)\s*(сек|секунд[уы]?|с|мин|минут[уы]?|м|час(?:а|ов)?|ч|дн(?:я|ей)?|день|д)\s*$",
        normalized,
    )
    if ru_match:
        value = int(ru_match.group(1))
        unit = ru_match.group(2)
        if value <= 0:
            return None
        if unit.startswith(("сек", "с")):
            return value
        if unit.startswith(("мин", "м")):
            return value * 60
        if unit.startswith(("час", "ч")):
            return value * 60 * 60
        return value * 24 * 60 * 60

    raw = normalized
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


def _main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Создать тест", callback_data="menu:block"),
                InlineKeyboardButton(text="Отложить", callback_data="menu:schedule"),
            ],
            [
                InlineKeyboardButton(text="Очередь", callback_data="menu:scheduled"),
                InlineKeyboardButton(text="Канал", callback_data="menu:channel"),
            ],
            [InlineKeyboardButton(text="Помощь", callback_data="menu:help")],
        ]
    )


def _cancel_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Отмена", callback_data="menu:cancel")]]
    )


def _delay_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Через 10 минут", callback_data="schedule:600"),
                InlineKeyboardButton(text="Через 1 час", callback_data="schedule:3600"),
            ],
            [
                InlineKeyboardButton(text="Через 3 часа", callback_data="schedule:10800"),
                InlineKeyboardButton(text="Завтра", callback_data="schedule:86400"),
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="menu:cancel")],
        ]
    )


def _builder_question_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Готово, перейти к отправке", callback_data="builder:finish")],
            [InlineKeyboardButton(text="Отмена", callback_data="menu:cancel")],
        ]
    )


def _builder_send_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Отправить сейчас", callback_data="builder:send_now"),
                InlineKeyboardButton(text="Отложить", callback_data="builder:schedule"),
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="menu:cancel")],
        ]
    )


def _builder_delay_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Через 1 час", callback_data="builder_delay:3600"),
                InlineKeyboardButton(text="Через 2 часа", callback_data="builder_delay:7200"),
            ],
            [
                InlineKeyboardButton(text="Завтра", callback_data="builder_delay:86400"),
                InlineKeyboardButton(text="Свое значение", callback_data="builder_delay:custom"),
            ],
            [InlineKeyboardButton(text="Отмена", callback_data="menu:cancel")],
        ]
    )


def _help_text() -> str:
    return (
        "Кнопка «Создать тест» запускает пошаговое создание: тема, сообщение перед тестом, вопросы по одному, затем отправка сейчас или отложенная отправка.\n"
        "Вопрос можно прислать текстом или фото с подписью в формате вопроса.\n\n"
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


def _scheduled_text(schedule_store: ScheduledQuizStore) -> str:
    jobs = schedule_store.list_all()
    if not jobs:
        return "Отложенных блоков пока нет."

    lines = ["Отложенные блоки:"]
    for job in jobs:
        topic = f" - {job.topic}" if job.topic else ""
        lines.append(f"{job.id[:8]}: {_format_when(job.send_at)}{topic} -> {job.channel_id}")
    return "\n".join(lines)


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


async def _send_parsed_quiz(
    *,
    bot: Bot,
    channel_id: str,
    parsed,
    poll_anonymous: bool,
    poll_multiple_answers: bool,
) -> None:
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
        return

    await bot.send_poll(
        chat_id=channel_id,
        question=parsed.question,
        options=parsed.options,
        type="quiz",
        correct_option_id=parsed.correct_option_id,
        is_anonymous=poll_anonymous,
        allows_multiple_answers=poll_multiple_answers,
    )


async def _post_built_quiz(
    *,
    bot: Bot,
    channel_id: str,
    intro_text: str | None,
    questions: list[ScheduledQuizQuestion],
    poll_anonymous: bool,
    poll_multiple_answers: bool,
) -> str:
    if intro_text:
        await bot.send_message(chat_id=channel_id, text=intro_text)
        await asyncio.sleep(0.4)

    for question in questions:
        parsed = parse_quiz_text(question.text)
        if question.photo_file_id:
            await bot.send_photo(chat_id=channel_id, photo=question.photo_file_id)
            await asyncio.sleep(0.4)
        await _send_parsed_quiz(
            bot=bot,
            channel_id=channel_id,
            parsed=parsed,
            poll_anonymous=poll_anonymous,
            poll_multiple_answers=poll_multiple_answers,
        )
        await asyncio.sleep(0.4)

    word = "вопрос" if len(questions) == 1 else "вопроса" if 2 <= len(questions) <= 4 else "вопросов"
    return f"Опубликовал тест: {len(questions)} {word} в канал {channel_id}."


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
        if job.questions:
            await _post_built_quiz(
                bot=bot,
                channel_id=job.channel_id,
                intro_text=job.intro_text,
                questions=job.questions,
                poll_anonymous=cfg.poll_anonymous,
                poll_multiple_answers=cfg.poll_multiple_answers,
            )
        else:
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
    pending_actions: dict[int, dict[str, object]] = {}

    async def ensure_channel(m: Message) -> str | None:
        if not m.from_user:
            return None

        channel_id = channel_store.get(m.from_user.id) or cfg.target_channel_id
        if channel_id:
            return channel_id

        users_waiting_for_channel.add(m.from_user.id)
        await _ask_channel_id(m)
        return None

    async def publish_block_from_text(
        *,
        m: Message,
        channel_id: str,
        text: str,
        topic: str | None,
    ) -> None:
        photo_file_id = photo_cache.pop_if_fresh(m.from_user.id)
        try:
            msg = await _post_quiz_block(
                bot=bot,
                channel_id=channel_id,
                question_text=text,
                topic=topic,
                photo_file_id=photo_file_id,
                poll_anonymous=cfg.poll_anonymous,
                poll_multiple_answers=cfg.poll_multiple_answers,
            )
        except ParseError as e:
            await m.answer(f"Не смог разобрать блок: {e}", reply_markup=_main_menu())
            return
        await m.answer(msg, reply_markup=_main_menu())

    async def schedule_block_from_text(
        *,
        m: Message,
        channel_id: str,
        text: str,
        delay_seconds: int,
        topic: str | None,
    ) -> None:
        photo_file_id = photo_cache.pop_if_fresh(m.from_user.id)
        try:
            block = parse_quiz_block_text(text, topic=topic)
        except ParseError as e:
            await m.answer(f"Не смог разобрать блок: {e}", reply_markup=_main_menu())
            return

        job = schedule_store.add(
            user_id=m.from_user.id,
            channel_id=channel_id,
            question_text=text,
            topic=block.topic,
            photo_file_id=photo_file_id,
            send_at=time.time() + delay_seconds,
        )
        _schedule_job_task(bot=bot, job=job, schedule_store=schedule_store, cfg=cfg)

        await m.answer(
            f"Поставил в очередь {len(block.quizzes)} вопросов"
            f"{f' по теме «{block.topic}»' if block.topic else ''}.\n"
            f"Отправка: {_format_when(job.send_at)}.\n"
            f"ID задачи: {job.id[:8]}",
            reply_markup=_main_menu(),
        )

    def new_builder_state() -> dict[str, object]:
        return {
            "mode": "builder_topic",
            "topic": None,
            "intro_text": None,
            "questions": [],
            "pending_photo_file_id": None,
        }

    async def start_builder_dialog(message: Message, user_id: int) -> None:
        pending_actions[user_id] = new_builder_state()
        await message.answer(
            "Напишите тему теста.\n\n"
            "Например: циклы",
            reply_markup=_cancel_menu(),
        )

    def builder_intro(topic: str) -> str:
        return f"Сейчас будет викторина по теме «{topic}»."

    def builder_questions(pending: dict[str, object]) -> list[ScheduledQuizQuestion]:
        questions = pending.get("questions")
        if isinstance(questions, list):
            return questions
        return []

    async def add_builder_question(
        *,
        m: Message,
        pending: dict[str, object],
        text: str,
        photo_file_id: str | None,
    ) -> None:
        try:
            parse_quiz_text(text)
        except ParseError as e:
            await m.answer(
                f"Не смог разобрать вопрос: {e}\n\n"
                "Формат: первая строка - вопрос, дальше варианты ответов. Правильный вариант отметьте * или +.",
                reply_markup=_builder_question_menu(),
            )
            return

        questions = builder_questions(pending)
        questions.append(ScheduledQuizQuestion(text=text, photo_file_id=photo_file_id))
        pending["questions"] = questions
        pending["pending_photo_file_id"] = None

        await m.answer(
            f"Добавил вопрос #{len(questions)}.\n\n"
            "Пришлите следующий вопрос текстом или фото с подписью. Когда вопросы закончатся, нажмите «Готово» или напишите «готово».",
            reply_markup=_builder_question_menu(),
        )

    async def show_builder_send_choice(m: Message, pending: dict[str, object]) -> None:
        questions = builder_questions(pending)
        if not questions:
            await m.answer("В тесте пока нет вопросов. Пришлите хотя бы один вопрос.", reply_markup=_builder_question_menu())
            return

        pending["mode"] = "builder_ready"
        await m.answer(
            f"Тест готов: {len(questions)} вопрос(ов).\n\n"
            "Отправить сейчас или отложить на какое-то время?",
            reply_markup=_builder_send_menu(),
        )

    async def publish_builder_now(
        *,
        message: Message,
        channel_id: str,
        pending: dict[str, object],
    ) -> None:
        questions = builder_questions(pending)
        try:
            msg = await _post_built_quiz(
                bot=bot,
                channel_id=channel_id,
                intro_text=str(pending.get("intro_text") or ""),
                questions=questions,
                poll_anonymous=cfg.poll_anonymous,
                poll_multiple_answers=cfg.poll_multiple_answers,
            )
        except ParseError as e:
            await message.answer(f"Не смог отправить тест: {e}", reply_markup=_main_menu())
            return
        await message.answer(msg, reply_markup=_main_menu())

    async def schedule_builder(
        *,
        message: Message,
        user_id: int,
        channel_id: str,
        pending: dict[str, object],
        delay_seconds: int,
    ) -> None:
        questions = builder_questions(pending)
        if not questions:
            await message.answer("В тесте пока нет вопросов.", reply_markup=_main_menu())
            return

        job = schedule_store.add(
            user_id=user_id,
            channel_id=channel_id,
            question_text="",
            topic=str(pending.get("topic") or "") or None,
            photo_file_id=None,
            intro_text=str(pending.get("intro_text") or "") or None,
            questions=questions,
            send_at=time.time() + delay_seconds,
        )
        _schedule_job_task(bot=bot, job=job, schedule_store=schedule_store, cfg=cfg)

        await message.answer(
            f"Поставил тест в очередь: {len(questions)} вопрос(ов).\n"
            f"Отправка: {_format_when(job.send_at)}.\n"
            f"ID задачи: {job.id[:8]}",
            reply_markup=_main_menu(),
        )

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
            "Для блоков, отложенной отправки и очереди используйте кнопки ниже.",
            reply_markup=_main_menu(),
        )

    @router.message(Command("channel"))
    async def channel_cmd(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return
        if not m.from_user:
            return

        pending_actions.pop(m.from_user.id, None)
        users_waiting_for_channel.add(m.from_user.id)
        await _ask_channel_id(m)

    @router.message(Command("help"))
    async def help_cmd(m: Message) -> None:
        await m.answer(_help_text(), reply_markup=_main_menu())

    @router.message(Command("scheduled"))
    async def scheduled_cmd(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return

        await m.answer(_scheduled_text(schedule_store), reply_markup=_main_menu())

    @router.message(Command("test"))
    async def test_cmd(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return
        if not m.from_user:
            return

        await start_builder_dialog(m, m.from_user.id)

    @router.callback_query(F.data == "menu:block")
    async def menu_block(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return
        if not c.from_user:
            await c.answer()
            return

        await start_builder_dialog(c.message, c.from_user.id)
        await c.answer()

    @router.callback_query(F.data == "menu:schedule")
    async def menu_schedule(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        await c.message.answer("Через сколько отправить блок?", reply_markup=_delay_menu())
        await c.answer()

    @router.callback_query(F.data.startswith("schedule:"))
    async def menu_schedule_delay(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return
        if not c.from_user:
            await c.answer()
            return

        raw_delay = (c.data or "").split(":", 1)[1]
        try:
            delay_seconds = int(raw_delay)
        except ValueError:
            await c.message.answer("Не понял задержку. Попробуйте еще раз.", reply_markup=_main_menu())
            await c.answer()
            return

        pending_actions[c.from_user.id] = {"mode": "schedule", "delay_seconds": delay_seconds}
        await c.message.answer(
            "Теперь пришлите блок вопросов одним сообщением.\n\n"
            "Тему можно написать первой строкой, например:\n"
            "Тема: История",
            reply_markup=_cancel_menu(),
        )
        await c.answer()

    @router.callback_query(F.data == "menu:scheduled")
    async def menu_scheduled(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        await c.message.answer(_scheduled_text(schedule_store), reply_markup=_main_menu())
        await c.answer()

    @router.callback_query(F.data == "menu:channel")
    async def menu_channel(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return
        if not c.from_user:
            await c.answer()
            return

        pending_actions.pop(c.from_user.id, None)
        users_waiting_for_channel.add(c.from_user.id)
        await c.message.answer(
            "Пришлите новый ID канала: -1001234567890 или @your_public_channel.",
            reply_markup=_cancel_menu(),
        )
        await c.answer()

    @router.callback_query(F.data == "menu:help")
    async def menu_help(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        await c.message.answer(_help_text(), reply_markup=_main_menu())
        await c.answer()

    @router.callback_query(F.data == "menu:cancel")
    async def menu_cancel(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return
        if c.from_user:
            pending_actions.pop(c.from_user.id, None)
            users_waiting_for_channel.discard(c.from_user.id)
        await c.message.answer("Ок, отменил текущее действие.", reply_markup=_main_menu())
        await c.answer()

    @router.callback_query(F.data == "builder:finish")
    async def builder_finish(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        pending = pending_actions.get(c.from_user.id)
        if not pending or pending.get("mode") not in {"builder_questions", "builder_ready"}:
            await c.message.answer("Сейчас нет теста в сборке.", reply_markup=_main_menu())
            await c.answer()
            return

        await show_builder_send_choice(c.message, pending)
        await c.answer()

    @router.callback_query(F.data == "builder:send_now")
    async def builder_send_now(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        pending = pending_actions.pop(c.from_user.id, None)
        if not pending or pending.get("mode") != "builder_ready":
            await c.message.answer("Сейчас нет готового теста.", reply_markup=_main_menu())
            await c.answer()
            return

        channel_id = channel_store.get(c.from_user.id) or cfg.target_channel_id
        if not channel_id:
            pending_actions[c.from_user.id] = pending
            users_waiting_for_channel.add(c.from_user.id)
            await _ask_channel_id(c.message)
            await c.answer()
            return

        await publish_builder_now(message=c.message, channel_id=channel_id, pending=pending)
        await c.answer()

    @router.callback_query(F.data == "builder:schedule")
    async def builder_schedule_choice(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        pending = pending_actions.get(c.from_user.id)
        if not pending or pending.get("mode") != "builder_ready":
            await c.message.answer("Сейчас нет готового теста.", reply_markup=_main_menu())
            await c.answer()
            return

        pending["mode"] = "builder_delay"
        await c.message.answer(
            "Через сколько отправить тест?\n\n"
            "Можно выбрать кнопку или потом написать свое значение: 30m, 1h, 2h, 1d.",
            reply_markup=_builder_delay_menu(),
        )
        await c.answer()

    @router.callback_query(F.data.startswith("builder_delay:"))
    async def builder_schedule_delay(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        pending = pending_actions.get(c.from_user.id)
        if not pending or pending.get("mode") not in {"builder_ready", "builder_delay"}:
            await c.message.answer("Сейчас нет готового теста.", reply_markup=_main_menu())
            await c.answer()
            return

        raw_delay = (c.data or "").split(":", 1)[1]
        if raw_delay == "custom":
            await c.message.answer(
                "Напишите задержку: например 30m, 1h, 2h или 1d. Если написать просто число, это будут минуты.",
                reply_markup=_cancel_menu(),
            )
            await c.answer()
            return

        try:
            delay_seconds = int(raw_delay)
        except ValueError:
            await c.message.answer("Не понял задержку. Попробуйте еще раз.", reply_markup=_builder_delay_menu())
            await c.answer()
            return

        pending_actions.pop(c.from_user.id, None)
        channel_id = channel_store.get(c.from_user.id) or cfg.target_channel_id
        if not channel_id:
            pending_actions[c.from_user.id] = pending
            users_waiting_for_channel.add(c.from_user.id)
            await _ask_channel_id(c.message)
            await c.answer()
            return

        await schedule_builder(
            message=c.message,
            user_id=c.from_user.id,
            channel_id=channel_id,
            pending=pending,
            delay_seconds=delay_seconds,
        )
        await c.answer()

    @router.message(Command("block"))
    async def block_cmd(m: Message, bot: Bot) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return
        if not m.from_user:
            return

        pending_actions.pop(m.from_user.id, None)
        channel_id = await ensure_channel(m)
        if not channel_id:
            return

        header, body = _split_command_payload(m.text or "")
        topic = _command_tail(header) or None
        if not body:
            await m.answer(
                "После /block добавьте блок вопросов. Например: /block История, а ниже вопросы.",
                reply_markup=_main_menu(),
            )
            return

        await publish_block_from_text(m=m, channel_id=channel_id, text=body, topic=topic)

    @router.message(Command("schedule"))
    async def schedule_cmd(m: Message) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return
        if not m.from_user:
            return

        pending_actions.pop(m.from_user.id, None)
        channel_id = await ensure_channel(m)
        if not channel_id:
            return

        header, body = _split_command_payload(m.text or "")
        args = _command_tail(header)
        if not args or not body:
            await m.answer(
                "Формат: /schedule 10m Тема\n"
                "Ниже добавьте блок вопросов. Время можно указать как 30s, 10m, 2h или 1d.",
                reply_markup=_main_menu(),
            )
            return

        parts = args.split(maxsplit=1)
        delay_seconds = _parse_delay(parts[0])
        if delay_seconds is None:
            await m.answer(
                "Не понял время. Используйте формат 30s, 10m, 2h или 1d.",
                reply_markup=_main_menu(),
            )
            return

        topic = parts[1].strip() if len(parts) > 1 else None
        await schedule_block_from_text(
            m=m,
            channel_id=channel_id,
            text=body,
            delay_seconds=delay_seconds,
            topic=topic,
        )

    @router.message(F.photo)
    async def on_photo(m: Message, bot: Bot) -> None:
        if not _is_allowed(m, cfg.admin_user_id):
            return

        if not m.from_user:
            return

        pending = pending_actions.get(m.from_user.id)
        if pending and pending.get("mode") == "builder_questions":
            photo = m.photo[-1]
            caption = (m.caption or "").strip()
            if caption:
                await add_builder_question(
                    m=m,
                    pending=pending,
                    text=caption,
                    photo_file_id=photo.file_id,
                )
                return

            pending["pending_photo_file_id"] = photo.file_id
            await m.answer(
                "Фото принял. Теперь пришлите текст этого вопроса: вопрос первой строкой, ниже варианты ответов.",
                reply_markup=_builder_question_menu(),
            )
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
            pending = pending_actions.get(m.from_user.id)
            if pending and pending.get("mode") in {"builder_ready", "builder_delay"}:
                pending["mode"] = "builder_ready"
                await m.answer(
                    f"Канал сохранен: {channel_id}\n\n"
                    "Можно отправить готовый тест сейчас или отложить.",
                    reply_markup=_builder_send_menu(),
                )
                return

            await m.answer(
                f"Канал сохранен: {channel_id}\n"
                "Теперь он используется по умолчанию. Чтобы заменить канал, отправьте /channel.",
                reply_markup=_main_menu(),
            )
            return

        if text.lower() in {"создать тест", "создать викторину", "новый тест"}:
            await start_builder_dialog(m, m.from_user.id)
            return

        pending = pending_actions.get(m.from_user.id)
        if pending:
            mode = pending.get("mode")

            if mode == "builder_topic":
                topic = text.strip()
                if not topic:
                    await m.answer("Напишите тему теста, например: циклы.", reply_markup=_cancel_menu())
                    return
                pending["topic"] = topic
                pending["intro_text"] = builder_intro(topic)
                pending["mode"] = "builder_intro"
                await m.answer(
                    f"Сообщение перед тестом:\n\n{pending['intro_text']}\n\n"
                    "Пришлите другой текст, если хотите заменить его, или напишите - чтобы оставить так.",
                    reply_markup=_cancel_menu(),
                )
                return

            if mode == "builder_intro":
                if text not in {"-", "—"}:
                    pending["intro_text"] = text
                pending["mode"] = "builder_questions"
                await m.answer(
                    "Теперь присылайте вопросы по одному.\n\n"
                    "Можно текстом:\n"
                    "Что выведет цикл?\n"
                    "*0 1 2\n"
                    "1 2 3\n\n"
                    "Или фото с такой подписью. Когда закончите, нажмите «Готово» или напишите «готово».",
                    reply_markup=_builder_question_menu(),
                )
                return

            if mode == "builder_questions":
                if text.lower() in {"готово", "done", "finish", "стоп"}:
                    if pending.get("pending_photo_file_id"):
                        await m.answer(
                            "К последнему фото еще нужен текст вопроса. Пришлите вопрос или отмените создание теста.",
                            reply_markup=_builder_question_menu(),
                        )
                        return
                    await show_builder_send_choice(m, pending)
                    return

                photo_file_id = pending.get("pending_photo_file_id")
                await add_builder_question(
                    m=m,
                    pending=pending,
                    text=text,
                    photo_file_id=str(photo_file_id) if photo_file_id else None,
                )
                return

            if mode == "builder_ready":
                delay_seconds = _parse_delay(text)
                channel_id = await ensure_channel(m)
                if not channel_id:
                    return

                pending_actions.pop(m.from_user.id, None)
                if text.lower() in {"сейчас", "отправить", "отправить сейчас", "send"}:
                    await publish_builder_now(message=m, channel_id=channel_id, pending=pending)
                    return
                if delay_seconds is not None:
                    await schedule_builder(
                        message=m,
                        user_id=m.from_user.id,
                        channel_id=channel_id,
                        pending=pending,
                        delay_seconds=delay_seconds,
                    )
                    return

                pending_actions[m.from_user.id] = pending
                await show_builder_send_choice(m, pending)
                return

            if mode == "builder_delay":
                delay_seconds = _parse_delay(text)
                if delay_seconds is None:
                    await m.answer(
                        "Не понял время. Используйте формат 30m, 1h, 2h или 1d.",
                        reply_markup=_cancel_menu(),
                    )
                    return

                channel_id = await ensure_channel(m)
                if not channel_id:
                    return

                pending_actions.pop(m.from_user.id, None)
                await schedule_builder(
                    message=m,
                    user_id=m.from_user.id,
                    channel_id=channel_id,
                    pending=pending,
                    delay_seconds=delay_seconds,
                )
                return

        channel_id = await ensure_channel(m)
        if not channel_id:
            return

        pending = pending_actions.pop(m.from_user.id, None)
        if pending:
            mode = pending.get("mode")
            if mode == "block":
                await publish_block_from_text(m=m, channel_id=channel_id, text=text, topic=None)
                return
            if mode == "schedule":
                delay_seconds = int(pending.get("delay_seconds", 0))
                if delay_seconds <= 0:
                    await m.answer("Не понял задержку. Попробуйте еще раз.", reply_markup=_main_menu())
                    return
                await schedule_block_from_text(
                    m=m,
                    channel_id=channel_id,
                    text=text,
                    delay_seconds=delay_seconds,
                    topic=None,
                )
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

    await bot.set_my_commands(
        [
            BotCommand(command="start", description="Открыть меню"),
            BotCommand(command="help", description="Показать формат вопросов"),
            BotCommand(command="test", description="Создать тест пошагово"),
            BotCommand(command="block", description="Опубликовать блок вопросов"),
            BotCommand(command="schedule", description="Отложить блок вопросов"),
            BotCommand(command="scheduled", description="Показать очередь"),
            BotCommand(command="channel", description="Сменить канал"),
        ]
    )

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
