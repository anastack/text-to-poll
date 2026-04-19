from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import time
from uuid import uuid4


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


@dataclass(frozen=True)
class ScheduledQuizQuestion:
    text: str
    photo_file_id: str | None = None


@dataclass(frozen=True)
class ScheduledQuizJob:
    id: str
    user_id: int
    channel_id: str
    question_text: str
    topic: str | None
    photo_file_id: str | None
    intro_text: str | None
    questions: list[ScheduledQuizQuestion]
    send_at: float
    created_at: float


class ScheduledQuizStore:
    def __init__(self, path: Path) -> None:
        self._path = path
        self._jobs = self._load()

    def add(
        self,
        *,
        user_id: int,
        channel_id: str,
        question_text: str,
        topic: str | None,
        photo_file_id: str | None,
        send_at: float,
        intro_text: str | None = None,
        questions: list[ScheduledQuizQuestion] | None = None,
    ) -> ScheduledQuizJob:
        job = ScheduledQuizJob(
            id=uuid4().hex,
            user_id=user_id,
            channel_id=channel_id,
            question_text=question_text,
            topic=topic,
            photo_file_id=photo_file_id,
            intro_text=intro_text,
            questions=questions or [],
            send_at=send_at,
            created_at=time.time(),
        )
        self._jobs[job.id] = job
        self._save()
        return job

    def remove(self, job_id: str) -> None:
        if job_id in self._jobs:
            del self._jobs[job_id]
            self._save()

    def list_all(self) -> list[ScheduledQuizJob]:
        return sorted(self._jobs.values(), key=lambda job: job.send_at)

    def _load(self) -> dict[str, ScheduledQuizJob]:
        if not self._path.exists():
            return {}
        try:
            raw = json.loads(self._path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(raw, list):
            return {}

        jobs: dict[str, ScheduledQuizJob] = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            try:
                job = ScheduledQuizJob(
                    id=str(item["id"]),
                    user_id=int(item["user_id"]),
                    channel_id=str(item["channel_id"]),
                    question_text=str(item.get("question_text", "")),
                    topic=str(item["topic"]) if item.get("topic") else None,
                    photo_file_id=str(item["photo_file_id"]) if item.get("photo_file_id") else None,
                    intro_text=str(item["intro_text"]) if item.get("intro_text") else None,
                    questions=[
                        ScheduledQuizQuestion(
                            text=str(question["text"]),
                            photo_file_id=str(question["photo_file_id"])
                            if question.get("photo_file_id")
                            else None,
                        )
                        for question in item.get("questions", [])
                        if isinstance(question, dict) and question.get("text")
                    ],
                    send_at=float(item["send_at"]),
                    created_at=float(item.get("created_at", time.time())),
                )
            except (KeyError, TypeError, ValueError):
                continue
            jobs[job.id] = job
        return jobs

    def _save(self) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = [
            {
                "id": job.id,
                "user_id": job.user_id,
                "channel_id": job.channel_id,
                "question_text": job.question_text,
                "topic": job.topic,
                "photo_file_id": job.photo_file_id,
                "intro_text": job.intro_text,
                "questions": [
                    {
                        "text": question.text,
                        "photo_file_id": question.photo_file_id,
                    }
                    for question in job.questions
                ],
                "send_at": job.send_at,
                "created_at": job.created_at,
            }
            for job in self.list_all()
        ]
        self._path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

