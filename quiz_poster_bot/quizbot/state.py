from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time


@dataclass
class PendingPhoto:
    file_id: str
    created_at: float


class PhotoCache:
    def __init__(self, ttl_seconds: int) -> None:
        self._ttl = max(1, ttl_seconds)
        self._by_user: dict[int, PendingPhoto] = {}

    def set(self, user_id: int, file_id: str) -> None:
        self._by_user[user_id] = PendingPhoto(file_id=file_id, created_at=time.time())

    def pop_if_fresh(self, user_id: int) -> str | None:
        p = self._by_user.pop(user_id, None)
        if not p:
            return None
        if time.time() - p.created_at > self._ttl:
            return None
        return p.file_id


class ChannelSelectionStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._by_user = self._load()

    def get(self, user_id: int) -> str | None:
        return self._by_user.get(str(user_id))

    def set(self, user_id: int, chat_id: str) -> None:
        self._by_user[str(user_id)] = chat_id
        self._save()

    def _load(self) -> dict[str, str]:
        if not self._path.exists():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, dict):
            return {}
        return {str(k): str(v) for k, v in raw.items() if v}

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(
            json.dumps(self._by_user, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

