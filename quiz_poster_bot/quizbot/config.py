from __future__ import annotations

from dataclasses import dataclass
import os

from dotenv import load_dotenv


def _as_bool(v: str, *, default: bool = False) -> bool:
    raw = (v or "").strip().lower()
    if raw == "":
        return default
    return raw in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Config:
    bot_token: str
    target_channel_id: str | None
    admin_user_id: int | None
    photo_ttl_seconds: int
    poll_anonymous: bool
    poll_multiple_answers: bool


def load_config() -> Config:
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is required. Put it into quiz_poster_bot/.env")

    target_channel_id = os.getenv("TARGET_CHANNEL_ID", "").strip() or None

    admin_user_id_raw = os.getenv("ADMIN_USER_ID", "").strip()
    admin_user_id: int | None
    if admin_user_id_raw:
        try:
            admin_user_id = int(admin_user_id_raw)
        except ValueError as e:
            raise RuntimeError("ADMIN_USER_ID must be integer") from e
    else:
        admin_user_id = None

    photo_ttl_raw = os.getenv("PHOTO_TTL_SECONDS", "600").strip()
    try:
        photo_ttl_seconds = int(photo_ttl_raw)
    except ValueError as e:
        raise RuntimeError("PHOTO_TTL_SECONDS must be integer") from e

    poll_anonymous = _as_bool(os.getenv("POLL_ANONYMOUS", "0"), default=False)
    poll_multiple_answers = _as_bool(os.getenv("POLL_MULTIPLE_ANSWERS", "0"), default=False)

    return Config(
        bot_token=bot_token,
        target_channel_id=target_channel_id,
        admin_user_id=admin_user_id,
        photo_ttl_seconds=photo_ttl_seconds,
        poll_anonymous=poll_anonymous,
        poll_multiple_answers=poll_multiple_answers,
    )
