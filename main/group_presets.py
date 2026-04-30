from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import Any

from tsu_schedule import GroupMatch


@dataclass(frozen=True)
class LecturePreset:
    key: str
    title: str
    url: str = ""
    professor: str = ""
    kind: str = ""
    enabled_by_default: bool = False


FACULTY_ID = "3d973de1-9bbc-11e7-813b-005056bc249c"
FACULTY_NAME = "Институт прикладной математики и компьютерных наук"

COMMON_LECTURES: list[dict[str, Any]] = [
    {
        "key": "algorithms",
        "title": "Алгоритмы и структуры данных",
        "professor": "Фукс Александр Львович",
        "kind": "Лекция / практика",
        "url": (
            "https://e-class.tsu.ru/#join:"
            "t3d870347-febe-45ab-929e-b98747aa494e,true"
        ),
        "enabled_by_default": True,
    },
    {
        "key": "optimization-methods",
        "title": "Методы оптимизации и исследование операций",
        "professor": "",
        "kind": "Лекция / практика",
        "url": (
            "https://e-class.tsu.ru/#join:"
            "te920dc11-0c92-47ed-91f6-1e43840fc91f,true"
        ),
        "enabled_by_default": True,
    },
    {
        "key": "computer-architecture",
        "title": "Архитектура вычислительных систем",
        "professor": "",
        "kind": "Лекция / практика",
        "url": (
            "https://e-class.tsu.ru/#join:"
            "ta2cc5fe5-f9d3-4b60-8546-2b26a4e199c0,true"
        ),
        "enabled_by_default": True,
    },
]


def _group(name: str, group_id: str) -> dict[str, Any]:
    return {
        "name": name,
        "faculty_id": FACULTY_ID,
        "group_id": group_id,
        "faculty_name": FACULTY_NAME,
        "schedule_url": (
            "https://intime.tsu.ru/schedule/group/"
            f"{FACULTY_ID}/{group_id}"
        ),
        "lectures": COMMON_LECTURES,
    }


SUPPORTED_GROUPS: dict[str, dict[str, Any]] = {
    "932401": _group("932401", "35e50016-2f00-11ef-815e-005056bc52bb"),
    "932402": _group("932402", "5527d477-2f00-11ef-815e-005056bc52bb"),
    "932403": _group("932403", "4d9906f9-2f03-11ef-815e-005056bc52bb"),
    "932404": _group("932404", "8410059a-2f03-11ef-815e-005056bc52bb"),
}

DEFAULT_GROUP_KEY = "932401"


def group_keys() -> list[str]:
    return list(SUPPORTED_GROUPS.keys())


def get_group(key: str | None = None) -> dict[str, Any] | None:
    if not key:
        return SUPPORTED_GROUPS.get(DEFAULT_GROUP_KEY)
    return SUPPORTED_GROUPS.get(key) or next(
        (group for group in SUPPORTED_GROUPS.values() if group["group_id"] == key),
        None,
    )


def get_group_match(key: str | None = None) -> GroupMatch:
    group = get_group(key)
    if not group:
        group = SUPPORTED_GROUPS[DEFAULT_GROUP_KEY]
    return GroupMatch(
        id=group["group_id"],
        name=group["name"],
        faculty_id=group["faculty_id"],
        faculty_name=group["faculty_name"],
        is_subgroup=False,
    )


def list_groups() -> list[dict[str, str]]:
    return [
        {
            "key": key,
            "name": group["name"],
            "faculty": group["faculty_name"],
            "schedule_url": group["schedule_url"],
        }
        for key, group in SUPPORTED_GROUPS.items()
    ]


def list_lecture_presets(group_key: str | None = None) -> list[LecturePreset]:
    group = get_group(group_key)
    if not group:
        return []
    return [LecturePreset(**item) for item in group["lectures"]]


def get_lecture_preset(group_key: str | None, preset_key: str) -> LecturePreset | None:
    return next(
        (preset for preset in list_lecture_presets(group_key) if preset.key == preset_key),
        None,
    )


def match_lecture(group_key: str | None, title: str, professor: str = "") -> LecturePreset | None:
    title_norm = _norm(title)
    professor_norm = _norm(professor)
    best: LecturePreset | None = None
    best_score = 0

    for preset in list_lecture_presets(group_key):
        preset_title = _norm(preset.title)
        if not preset_title:
            continue

        if title_norm == preset_title:
            score = 100
        elif preset_title in title_norm or title_norm in preset_title:
            score = 80
        else:
            continue

        preset_professor = _norm(preset.professor)
        if preset_professor and professor_norm and preset_professor == professor_norm:
            score += 10

        if score > best_score:
            best = preset
            best_score = score

    return best


def _norm(value: str) -> str:
    value = unicodedata.normalize("NFKC", value or "").casefold().replace("ё", "е")
    return re.sub(r"[\s_\-.,;:!?()\"'«»]+", "", value)
