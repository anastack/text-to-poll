from __future__ import annotations

from dataclasses import dataclass


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


class ParseError(ValueError):
    pass


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

