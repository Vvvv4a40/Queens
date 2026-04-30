from __future__ import annotations

import json
import logging
import re
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pytz

from config import TIMEZONE, TSU_API_BASE

log = logging.getLogger(__name__)
TZ = pytz.timezone(TIMEZONE)

HTTP_TIMEOUT = 12
GROUP_SEARCH_LIMIT = 10
URL_RE = re.compile(r"https?://[^\s<>'\"\\\]\)]+", re.IGNORECASE)

ONLINE_URL_HINTS = (
    "e-class.tsu.ru",
    "bbb",
    "bigbluebutton",
    "zoom.us",
    "teams.microsoft",
    "meet.google",
    "webex",
    "webinar",
)
ONLINE_TEXT_HINTS = (
    "online",
    "онлайн",
    "дистанц",
    "вкс",
    "вебинар",
    "e-class",
    "zoom",
    "teams",
    "webex",
)
class TsuScheduleError(RuntimeError):
    pass


@dataclass(frozen=True)
class GroupMatch:
    id: str
    name: str
    faculty_id: str
    faculty_name: str
    is_subgroup: bool = False

    @property
    def label(self) -> str:
        suffix = " (подгруппа)" if self.is_subgroup else ""
        return f"{self.name}{suffix} - {self.faculty_name}"


@dataclass(frozen=True)
class OnlineLesson:
    external_id: str
    title: str
    url: str
    start_dt: str
    duration_min: int
    audience: str
    professor: str
    lesson_type: str


_faculties_cache: list[dict[str, Any]] | None = None
_groups_cache: list[GroupMatch] | None = None


def find_groups(query: str, limit: int = GROUP_SEARCH_LIMIT) -> list[GroupMatch]:
    url_group = group_from_schedule_url(query)
    if url_group:
        return [url_group]

    query_norm = _normalize(query)
    if not query_norm:
        return []

    groups = load_groups()
    exact = [g for g in groups if _normalize(g.name) == query_norm]
    if exact:
        return exact[:limit]

    starts = [g for g in groups if _normalize(g.name).startswith(query_norm)]
    contains = [
        g for g in groups
        if query_norm in _normalize(g.name) and g not in starts
    ]
    return (starts + contains)[:limit]


def group_from_schedule_url(value: str) -> GroupMatch | None:
    match = re.search(
        r"/schedule/group/([0-9a-f-]{36})/([0-9a-f-]{36})",
        value,
        re.IGNORECASE,
    )
    if not match:
        return None

    faculty_id, group_id = match.groups()
    faculty = next(
        (f for f in fetch_faculties() if str(f.get("id")).lower() == faculty_id.lower()),
        None,
    )
    faculty_name = str(faculty.get("name")) if faculty else "Факультет из ссылки"

    try:
        for group in _request_json(f"v1/faculties/{faculty_id}/groups"):
            if str(group.get("id")).lower() == group_id.lower():
                return GroupMatch(
                    id=str(group["id"]),
                    name=str(group["name"]),
                    faculty_id=str(group.get("facultyId") or faculty_id),
                    faculty_name=faculty_name,
                    is_subgroup=bool(group.get("isSubgroup", False)),
                )
    except Exception as exc:
        log.warning("Cannot resolve group from schedule URL: %s", exc)

    return GroupMatch(
        id=group_id,
        name=group_id,
        faculty_id=faculty_id,
        faculty_name=faculty_name,
        is_subgroup=False,
    )


def load_groups(force: bool = False) -> list[GroupMatch]:
    global _groups_cache
    if _groups_cache is not None and not force:
        return _groups_cache

    groups: list[GroupMatch] = []
    faculties = fetch_faculties(force=force)
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(_load_faculty_groups, faculty): faculty
            for faculty in faculties
        }
        for future in as_completed(futures):
            faculty = futures[future]
            faculty_id = str(faculty.get("id"))
            try:
                groups.extend(future.result())
            except Exception as exc:
                log.warning("Cannot load groups for faculty %s: %s", faculty_id, exc)

    _groups_cache = sorted(groups, key=lambda g: (_normalize(g.name), g.faculty_name))
    return _groups_cache


def _load_faculty_groups(faculty: dict[str, Any]) -> list[GroupMatch]:
    faculty_id = str(faculty["id"])
    faculty_name = str(faculty["name"])
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            result = []
            for group in _request_json(f"v1/faculties/{faculty_id}/groups"):
                result.append(
                    GroupMatch(
                        id=str(group["id"]),
                        name=str(group["name"]),
                        faculty_id=str(group.get("facultyId") or faculty_id),
                        faculty_name=faculty_name,
                        is_subgroup=bool(group.get("isSubgroup", False)),
                    )
                )
            return result
        except Exception as exc:
            last_exc = exc
            if attempt == 0:
                time.sleep(0.4)
    raise TsuScheduleError(f"{faculty_name}: {last_exc}")


def fetch_faculties(force: bool = False) -> list[dict[str, Any]]:
    global _faculties_cache
    if _faculties_cache is not None and not force:
        return _faculties_cache

    data = _request_json("v1/faculties")
    if isinstance(data, dict):
        data = data.get("value") or data.get("items") or []
    if not isinstance(data, list):
        raise TsuScheduleError("Unexpected faculties response")

    _faculties_cache = data
    return _faculties_cache


def fetch_online_lessons(
    group_id: str,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[OnlineLesson]:
    date_from = date_from or datetime.now(TZ).date()
    date_to = date_to or (date_from + timedelta(days=14))
    params = urlencode(
        {
            "id": group_id,
            "dateFrom": date_from.isoformat(),
            "dateTo": date_to.isoformat(),
        }
    )
    schedule = _request_json(f"v1/schedule/group?{params}")
    return extract_online_lessons(schedule)


def extract_online_lessons(schedule: dict[str, Any]) -> list[OnlineLesson]:
    result: list[OnlineLesson] = []
    for day in schedule.get("grid", []):
        day_value = day.get("date")
        if not day_value:
            continue
        for lesson in day.get("lessons", []):
            if lesson.get("type") != "LESSON":
                continue

            join_url = extract_join_url(lesson)
            if not join_url and not _looks_online(lesson):
                continue

            start_dt = _lesson_datetime(str(day_value), int(lesson["starts"]))
            end_dt = _lesson_datetime(str(day_value), int(lesson["ends"]))
            if end_dt <= start_dt:
                end_dt += timedelta(days=1)

            duration_min = max(10, int((end_dt - start_dt).total_seconds() // 60))
            title = str(lesson.get("title") or "Онлайн-занятие")
            external_id = _external_lesson_id(lesson, str(day_value))
            result.append(
                OnlineLesson(
                    external_id=external_id,
                    title=title,
                    url=join_url or "",
                    start_dt=start_dt.strftime("%Y-%m-%d %H:%M"),
                    duration_min=duration_min,
                    audience=_audience_name(lesson),
                    professor=_professor_name(lesson),
                    lesson_type=str(lesson.get("lessonType") or ""),
                )
            )
    return result


def extract_join_url(lesson: dict[str, Any]) -> str | None:
    candidates: list[str] = []
    for value in _walk_strings(lesson):
        for match in URL_RE.finditer(value):
            url = _clean_url(match.group(0))
            if url and url not in candidates:
                candidates.append(url)

    if not candidates:
        return None

    online_candidates = [url for url in candidates if _is_online_url(url)]
    if online_candidates:
        online_candidates.sort(key=_url_priority)
        return online_candidates[0]

    if _looks_online(lesson):
        fallback_candidates = [url for url in candidates if not _is_noise_url(url)]
        fallback_candidates.sort(key=_url_priority)
        return fallback_candidates[0] if fallback_candidates else None

    return None


def _request_json(path: str) -> Any:
    url = f"{TSU_API_BASE.rstrip('/')}/{path.lstrip('/')}"
    req = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "TSU-Lecture-Bot/1.0",
        },
    )
    last_exc: Exception | None = None
    for attempt in range(2):
        try:
            with urlopen(req, timeout=HTTP_TIMEOUT) as response:
                raw = response.read().decode("utf-8")
            break
        except Exception as exc:
            last_exc = exc
            if attempt == 0:
                time.sleep(0.25)
    else:
        raise TsuScheduleError(f"Cannot fetch {url}: {last_exc}") from last_exc

    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TsuScheduleError(f"Invalid JSON from {url}") from exc


def _lesson_datetime(day_value: str, seconds: int) -> datetime:
    local_time = datetime.fromtimestamp(seconds, TZ).time().replace(second=0, microsecond=0)
    return datetime.combine(date.fromisoformat(day_value), local_time)


def _external_lesson_id(lesson: dict[str, Any], day_value: str) -> str:
    parts = [
        str(lesson.get("id") or "no-id"),
        day_value,
        str(lesson.get("starts") or ""),
        str(lesson.get("ends") or ""),
    ]
    return "|".join(parts)


def _audience_name(lesson: dict[str, Any]) -> str:
    audience = lesson.get("audience") or {}
    building = audience.get("building") or {}
    building_name = building.get("name")
    audience_name = audience.get("name") or ""
    if building_name:
        return f"{audience_name} ({building_name})".strip()
    return str(audience_name)


def _professor_name(lesson: dict[str, Any]) -> str:
    professor = lesson.get("professor") or {}
    return str(professor.get("fullName") or "")


def _walk_strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        result: list[str] = []
        for item in value.values():
            result.extend(_walk_strings(item))
        return result
    if isinstance(value, list):
        result: list[str] = []
        for item in value:
            result.extend(_walk_strings(item))
        return result
    return []


def _clean_url(url: str) -> str:
    return url.rstrip(".,;:!?)]}").strip()


def _url_priority(url: str) -> tuple[int, str]:
    lowered = url.lower()
    for hint in ONLINE_URL_HINTS:
        if hint in lowered:
            return (0, lowered)
    return (1, lowered)


def _is_online_url(url: str) -> bool:
    lowered = url.lower()
    return any(hint in lowered for hint in ONLINE_URL_HINTS)


def _is_noise_url(url: str) -> bool:
    lowered = url.lower()
    return (
        "/api/static/" in lowered
        or "/assets/" in lowered
        or lowered.endswith((".png", ".jpg", ".jpeg", ".gif", ".svg", ".ico", ".css", ".js", ".woff", ".woff2"))
    )


def _looks_online(lesson: dict[str, Any]) -> bool:
    text = " ".join(_walk_strings(lesson)).casefold()
    return any(hint in text for hint in ONLINE_TEXT_HINTS)


def _normalize(value: str) -> str:
    value = unicodedata.normalize("NFKC", value).casefold().replace("ё", "е")
    return re.sub(r"[\s_\-]+", "", value)
