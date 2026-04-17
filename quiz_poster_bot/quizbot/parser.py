from __future__ import annotations

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class ParsedQuiz:
    question: str
    options: list[str]
    correct_option_ids: list[int]

    @property
    def correct_option_id(self) -> int:
        return self.correct_option_ids[0]

    @property
    def has_multiple_correct_options(self) -> bool:
        return len(self.correct_option_ids) > 1


@dataclass(frozen=True)
class ParsedQuizBlock:
    topic: str | None
    quizzes: list[ParsedQuiz]


class ParseError(ValueError):
    pass


_TOPIC_PREFIX_RE = re.compile(r"^(?:тема|topic)\s*:\s*(.+)$", re.IGNORECASE)


def parse_quiz_text(text: str) -> ParsedQuiz:
    """
    Format:
    - first non-empty line: question
    - next non-empty lines: options (2..10)
    - correct option is marked with leading '*' or '+'
    """
    if not text or not text.strip():
        raise ParseError("Пустой текст.")

    lines = [ln.strip() for ln in text.replace("\r\n", "\n").split("\n")]
    lines = [ln for ln in lines if ln]
    if len(lines) < 3:
        raise ParseError("Нужно минимум 1 вопрос и 2 варианта ответа (в отдельных строках).")

    question = lines[0]
    raw_options = lines[1:]

    options: list[str] = []
    correct_indices: list[int] = []
    for raw in raw_options:
        is_correct = raw.startswith("*") or raw.startswith("+")
        opt = raw[1:].strip() if is_correct else raw
        if not opt:
            continue
        if is_correct:
            correct_indices.append(len(options))
        options.append(opt)

    if not question:
        raise ParseError("Вопрос пустой.")
    if len(options) < 2:
        raise ParseError("Нужно минимум 2 варианта ответа.")
    if len(options) > 10:
        raise ParseError("Telegram поддерживает максимум 10 вариантов. Укоротите список.")

    if not correct_indices:
        correct_indices = [0]

    return ParsedQuiz(question=question, options=options, correct_option_ids=correct_indices)


def extract_topic(text: str) -> tuple[str | None, str]:
    lines = text.replace("\r\n", "\n").split("\n")
    while lines and not lines[0].strip():
        lines.pop(0)

    if not lines:
        return None, ""

    first = lines[0].strip()
    match = _TOPIC_PREFIX_RE.match(first)
    if match:
        return match.group(1).strip(), "\n".join(lines[1:]).strip()

    if first.startswith("#"):
        topic = first.lstrip("#").strip()
        if topic:
            return topic, "\n".join(lines[1:]).strip()

    return None, text.strip()


def parse_quiz_block_text(text: str, *, topic: str | None = None) -> ParsedQuizBlock:
    detected_topic, body = extract_topic(text)
    topic = topic or detected_topic
    body = body.strip()
    if not body:
        raise ParseError("Пустой блок вопросов.")

    chunks = _split_quiz_chunks(body)
    quizzes: list[ParsedQuiz] = []
    errors: list[str] = []
    for idx, chunk in enumerate(chunks, start=1):
        try:
            quizzes.append(parse_quiz_text(chunk))
        except ParseError as e:
            errors.append(f"{idx}: {e}")

    if errors:
        raise ParseError("Не смог разобрать вопросы: " + "; ".join(errors))
    if not quizzes:
        raise ParseError("В блоке нет вопросов.")

    return ParsedQuizBlock(topic=topic, quizzes=quizzes)


def _split_quiz_chunks(text: str) -> list[str]:
    normalized = text.replace("\r\n", "\n").replace("\r", "\n").strip()
    if not normalized:
        return []

    by_separator = re.split(r"(?m)^\s*---+\s*$", normalized)
    chunks = [chunk.strip() for chunk in by_separator if chunk.strip()]
    if len(chunks) > 1:
        return chunks

    by_blank_lines = re.split(r"\n\s*\n+", normalized)
    chunks = [chunk.strip() for chunk in by_blank_lines if chunk.strip()]
    return chunks or [normalized]

