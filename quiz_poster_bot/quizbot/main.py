from __future__ import annotations

import asyncio
from datetime import datetime
import logging
from pathlib import Path
import re
import time

from aiogram import Bot, Dispatcher, F, Router
from aiogram.exceptions import TelegramBadRequest, TelegramNetworkError, TelegramRetryAfter, TelegramServerError
from aiogram.filters import Command, CommandStart
from aiogram.types import BotCommand, CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message

from .config import load_config
from .parser import ParseError, parse_quiz_block_text, parse_quiz_text
from .state import (
    ChannelSelectionStore,
    ScheduledQuizJob,
    ScheduledQuizQuestion,
    ScheduledQuizStore,
)


logger = logging.getLogger(__name__)
_POST_PAUSE_SECONDS = 3.2
_MAX_TELEGRAM_SEND_ATTEMPTS = 5
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


async def _channel_display_name(bot: Bot, channel_id: str) -> str:
    try:
        chat = await _send_telegram(bot.get_chat, chat_id=channel_id)
    except TelegramBadRequest:
        logger.warning("Could not resolve channel title for %s", channel_id, exc_info=True)
        return channel_id

    title = getattr(chat, "title", None)
    username = getattr(chat, "username", None)
    if title:
        return str(title)
    if username:
        return f"@{username}"
    return channel_id


def _main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Создать тест", callback_data="menu:test")],
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


def _single_question_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Отправить вопрос", callback_data="single:send"),
                InlineKeyboardButton(text="Отмена", callback_data="menu:cancel"),
            ],
        ]
    )


def _help_text() -> str:
    return (
        "Кнопка «Создать тест» запускает пошаговое создание: тема, сообщение перед тестом, вопросы по одному, затем отправка сейчас или отложенная отправка.\n"
        "Вопрос можно прислать текстом или фото с подписью в формате вопроса.\n\n"
        "Формат одного вопроса:\n"
        "1-я строка - вопрос\n"
        "между вопросом и ответами можно оставлять пустые строки и пробелы\n"
        "дальше - варианты ответа (2-10)\n"
        "правильный вариант обязательно отметьте * или + в начале строки\n\n"
        "Пример:\n"
        "Что выведет цикл?\n\n"
        "*0 1 2\n"
        "1 2 3\n"
        "ошибка"
    )


def _scheduled_text(schedule_store: ScheduledQuizStore) -> str:
    jobs = schedule_store.list_all()
    if not jobs:
        return "Отложенных тестов пока нет."

    lines = ["Отложенные тесты:"]
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
        await _send_telegram(
            bot.send_poll,
            chat_id=channel_id,
            question=parsed.question,
            options=parsed.options,
            type="regular",
            is_anonymous=poll_anonymous,
            allows_multiple_answers=True,
        )
        return

    await _send_telegram(
        bot.send_poll,
        chat_id=channel_id,
        question=parsed.question,
        options=parsed.options,
        type="quiz",
        correct_option_id=parsed.correct_option_id,
        is_anonymous=poll_anonymous,
        allows_multiple_answers=poll_multiple_answers,
    )


async def _send_telegram(method, *args, **kwargs):
    for attempt in range(1, _MAX_TELEGRAM_SEND_ATTEMPTS + 1):
        try:
            return await method(*args, **kwargs)
        except TelegramRetryAfter as e:
            delay = max(float(e.retry_after), 1.0) + 0.5
            logger.warning(
                "Telegram flood limit while calling %s; retrying in %.1f seconds",
                getattr(method, "__name__", method.__class__.__name__),
                delay,
            )
            await asyncio.sleep(delay)
        except (TelegramNetworkError, TelegramServerError):
            if attempt >= _MAX_TELEGRAM_SEND_ATTEMPTS:
                raise
            delay = float(attempt * 2)
            logger.warning(
                "Temporary Telegram error while calling %s; attempt %s/%s, retrying in %.1f seconds",
                getattr(method, "__name__", method.__class__.__name__),
                attempt,
                _MAX_TELEGRAM_SEND_ATTEMPTS,
                delay,
                exc_info=True,
            )
            await asyncio.sleep(delay)

    return await method(*args, **kwargs)


async def _post_built_quiz(
    *,
    bot: Bot,
    channel_id: str,
    intro_text: str | None,
    questions: list[ScheduledQuizQuestion],
    poll_anonymous: bool,
    poll_multiple_answers: bool,
    start_index: int = 0,
    on_question_posted=None,
) -> str:
    start_index = max(0, min(start_index, len(questions)))

    if intro_text and start_index == 0:
        await _send_telegram(bot.send_message, chat_id=channel_id, text=intro_text)
        await asyncio.sleep(_POST_PAUSE_SECONDS)

    for question_number, question in enumerate(questions[start_index:], start=start_index + 1):
        parsed = parse_quiz_text(question.text)
        if question.photo_file_id:
            await _send_telegram(bot.send_photo, chat_id=channel_id, photo=question.photo_file_id)
            await asyncio.sleep(_POST_PAUSE_SECONDS)
        await _send_parsed_quiz(
            bot=bot,
            channel_id=channel_id,
            parsed=parsed,
            poll_anonymous=poll_anonymous,
            poll_multiple_answers=poll_multiple_answers,
        )
        if on_question_posted:
            on_question_posted(question_number)
        await asyncio.sleep(_POST_PAUSE_SECONDS)

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
        await _send_telegram(bot.send_photo, chat_id=channel_id, photo=photo_file_id)
        await asyncio.sleep(_POST_PAUSE_SECONDS)

    if block.topic:
        await _send_telegram(bot.send_message, chat_id=channel_id, text=f"Тема: {block.topic}")
        await asyncio.sleep(_POST_PAUSE_SECONDS)

    for parsed in block.quizzes:
        if channel_id.startswith("@") or channel_id.startswith("-100"):
            poll_anonymous = True

        if parsed.has_multiple_correct_options:
            await _send_telegram(
                bot.send_poll,
                chat_id=channel_id,
                question=parsed.question,
                options=parsed.options,
                type="regular",
                is_anonymous=poll_anonymous,
                allows_multiple_answers=True,
            )
        else:
            await _send_telegram(
                bot.send_poll,
                chat_id=channel_id,
                question=parsed.question,
                options=parsed.options,
                type="quiz",
                correct_option_id=parsed.correct_option_id,
                is_anonymous=poll_anonymous,
                allows_multiple_answers=poll_multiple_answers,
            )
        await asyncio.sleep(_POST_PAUSE_SECONDS)

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
                start_index=job.published_question_count,
                on_question_posted=lambda count: schedule_store.mark_progress(job.id, count),
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
            if photo_file_id:
                pending["pending_photo_file_id"] = photo_file_id

            error_text = str(e)
            if "пометьте правильный ответ" in error_text:
                await m.answer(
                    error_text,
                    reply_markup=_builder_question_menu(),
                )
                return

            await m.answer(
                f"Не смог разобрать вопрос: {error_text}\n\n"
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

    async def show_single_question_choice(message: Message, question_text: str, channel_id: str) -> None:
        parsed = parse_quiz_text(question_text)
        channel_name = await _channel_display_name(bot, channel_id)
        await message.answer(
            f"Похоже, это одиночный вопрос:\n\n{parsed.question}\n\n"
            f"Отправить его в канал {channel_name}?",
            reply_markup=_single_question_menu(),
        )

    async def publish_single_question(
        *,
        message: Message,
        channel_id: str,
        question_text: str,
    ) -> None:
        try:
            parsed = parse_quiz_text(question_text)
            await _send_parsed_quiz(
                bot=bot,
                channel_id=channel_id,
                parsed=parsed,
                poll_anonymous=cfg.poll_anonymous,
                poll_multiple_answers=cfg.poll_multiple_answers,
            )
        except ParseError as e:
            await message.answer(f"Не смог отправить вопрос: {e}", reply_markup=_main_menu())
            return

        channel_name = await _channel_display_name(bot, channel_id)
        await message.answer(f"Отправил одиночный вопрос в канал {channel_name}.", reply_markup=_main_menu())

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

        channel_name = await _channel_display_name(bot, channel_id)
        await m.answer(
            f"Текущий канал: {channel_name}\n\n"
            "Создавайте тест пошагово: тема, сообщение перед тестом, вопросы по одному, затем отправка сейчас или отложенная отправка.",
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

    @router.callback_query(F.data == "menu:test")
    async def menu_test(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return
        if not c.from_user:
            await c.answer()
            return

        await start_builder_dialog(c.message, c.from_user.id)
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

    @router.callback_query(F.data == "single:send")
    async def single_send(c: CallbackQuery) -> None:
        if not c.message or not _is_callback_allowed(c, cfg.admin_user_id):
            await c.answer()
            return

        pending = pending_actions.get(c.from_user.id)
        if not pending or pending.get("mode") != "single_question_confirm":
            await c.message.answer("Сейчас нет одиночного вопроса для отправки.", reply_markup=_main_menu())
            await c.answer()
            return

        channel_id = channel_store.get(c.from_user.id) or cfg.target_channel_id
        if not channel_id:
            users_waiting_for_channel.add(c.from_user.id)
            await _ask_channel_id(c.message)
            await c.answer()
            return

        pending_actions.pop(c.from_user.id, None)
        await publish_single_question(
            message=c.message,
            channel_id=channel_id,
            question_text=str(pending.get("question_text") or ""),
        )
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

        await m.answer(
            "Фото добавляется только внутри создания теста. Нажмите «Создать тест» или отправьте /test, затем добавьте вопрос с картинкой.",
            reply_markup=_main_menu(),
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
            channel_name = await _channel_display_name(bot, channel_id)
            pending = pending_actions.get(m.from_user.id)
            if pending and pending.get("mode") == "single_question_confirm":
                question_text = str(pending.get("question_text") or "")
                await m.answer(f"Канал сохранен: {channel_name}")
                await show_single_question_choice(m, question_text, channel_id)
                return

            if pending and pending.get("mode") in {"builder_ready", "builder_delay"}:
                pending["mode"] = "builder_ready"
                await m.answer(
                    f"Канал сохранен: {channel_name}\n\n"
                    "Можно отправить готовый тест сейчас или отложить.",
                    reply_markup=_builder_send_menu(),
                )
                return

            await m.answer(
                f"Канал сохранен: {channel_name}\n"
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

            if mode == "single_question_confirm":
                normalized = text.lower()
                if normalized in {"да", "ок", "окей", "отправить", "send", "yes"}:
                    channel_id = await ensure_channel(m)
                    if not channel_id:
                        return

                    pending_actions.pop(m.from_user.id, None)
                    await publish_single_question(
                        message=m,
                        channel_id=channel_id,
                        question_text=str(pending.get("question_text") or ""),
                    )
                    return

                if normalized in {"нет", "отмена", "cancel", "no"}:
                    pending_actions.pop(m.from_user.id, None)
                    await m.answer("Ок, не отправляю одиночный вопрос.", reply_markup=_main_menu())
                    return

                await m.answer(
                    "Отправить этот одиночный вопрос? Нажмите кнопку или напишите «да» / «нет».",
                    reply_markup=_single_question_menu(),
                )
                return

        try:
            parse_quiz_text(text)
        except ParseError:
            pass
        else:
            channel_id = await ensure_channel(m)
            if not channel_id:
                pending_actions[m.from_user.id] = {
                    "mode": "single_question_confirm",
                    "question_text": text,
                }
                return

            pending_actions[m.from_user.id] = {
                "mode": "single_question_confirm",
                "question_text": text,
            }
            await show_single_question_choice(m, text, channel_id)
            return

        await m.answer(
            "Чтобы создать тест, нажмите «Создать тест» или отправьте /test. Вопросы теперь добавляются только по одному в этом режиме.",
            reply_markup=_main_menu(),
        )

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
            BotCommand(command="scheduled", description="Показать очередь"),
            BotCommand(command="channel", description="Сменить канал"),
        ]
    )

    base_path = Path(__file__).resolve().parent.parent
    channel_store = ChannelSelectionStore(base_path / "channel_selections.json")
    schedule_store = ScheduledQuizStore(base_path / "scheduled_quizzes.json")

    for job in schedule_store.list_all():
        _schedule_job_task(bot=bot, job=job, schedule_store=schedule_store, cfg=cfg)

    dp.include_router(
        build_router(
            bot=bot,
            channel_store=channel_store,
            schedule_store=schedule_store,
            cfg=cfg,
        )
    )

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
