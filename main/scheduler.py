import asyncio
import logging
from datetime import datetime, timedelta

import pytz
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date      import DateTrigger
from apscheduler.triggers.interval  import IntervalTrigger

import browser
import database as db
import group_presets
import tsu_schedule
from config import SCHEDULE_LOOKAHEAD_DAYS, SCHEDULE_SYNC_INTERVAL_MINUTES, TIMEZONE

log = logging.getLogger(__name__)
TZ  = pytz.timezone(TIMEZONE)

_scheduler: AsyncIOScheduler | None = None
_bot                                 = None   # telegram bot, для уведомлений
_planned: set[int]                   = set()  # id лекций которые уже запланированы


def start(bot):
    global _scheduler, _bot
    _bot = bot
    browser.set_auto_disconnect_callback(_on_chat_threshold_disconnect)

    _scheduler = AsyncIOScheduler(timezone=TZ)

    # Каждые 5 минут проверяем БД — вдруг добавили новую лекцию
    _scheduler.add_job(
        _check_and_plan,
        IntervalTrigger(minutes=5),
        id="check", replace_existing=True
    )
    _scheduler.add_job(
        _sync_tsu_schedules,
        IntervalTrigger(minutes=SCHEDULE_SYNC_INTERVAL_MINUTES),
        id="sync_tsu", replace_existing=True
    )
    _scheduler.start()
    log.info("Планировщик запущен (%s)", TIMEZONE)

    # Планируем сразу при старте
    asyncio.create_task(_sync_tsu_schedules())
    asyncio.create_task(_check_and_plan())


def stop():
    if _scheduler:
        _scheduler.shutdown(wait=False)


async def _check_and_plan():
    """
    Смотрит все активные лекции в БД и планирует задачи
    на подключение / отключение.
    Вызывается каждые 5 минут.
    """
    if not _scheduler:
        return

    lectures = db.get_all_active()
    now      = datetime.now(TZ)

    for lec in lectures:
        lid = lec["id"]
        if lid in _planned:
            continue   # Уже запланировано

        # Парсим дату из БД ("2026-03-15 08:45") в aware datetime
        # strptime — парсит строку по формату
        # %Y = год, %m = месяц, %d = день, %H = час, %M = минуты
        start_naive = datetime.strptime(lec["start_dt"], "%Y-%m-%d %H:%M")

        # localize() — говорит что это томское время (добавляет UTC+7)
        start_dt = TZ.localize(start_naive)

        # Время выхода = начало + длительность
        end_dt = start_dt + timedelta(minutes=lec["duration_min"])

        # Лекция уже полностью прошла — деактивируем, не планируем
        if end_dt < now:
            db.deactivate(lid)
            _planned.discard(lid)
            log.info("Лекция #%d уже прошла, деактивирована", lid)
            continue

        if not (lec.get("url") or "").strip():
            alert_dt = start_dt - timedelta(minutes=10)
            if alert_dt <= now <= end_dt:
                asyncio.create_task(_do_missing_link_alert(lec))
            elif alert_dt > now:
                _scheduler.add_job(
                    _do_missing_link_alert,
                    DateTrigger(run_date=alert_dt),
                    id=f"m_{lid}",
                    args=[lec],
                    replace_existing=True
                )
            _scheduler.add_job(
                _finish_missing_link_lecture,
                DateTrigger(run_date=end_dt),
                id=f"d_{lid}",
                args=[lec],
                replace_existing=True
            )
            _planned.add(lid)
            log.info("Лекция #%d онлайн, но без ссылки; ждём привязку", lid)
            continue

        # Планируем подключение (если ещё не началась)
        if start_dt > now:
            _scheduler.add_job(
                _do_connect,
                DateTrigger(run_date=start_dt),
                id=f"c_{lid}",
                args=[lec],
                replace_existing=True
            )
            log.info("Запланировано подключение #%d в %s",
                     lid, start_dt.strftime("%d.%m %H:%M"))
        else:
            # Лекция уже идёт — подключаемся немедленно
            asyncio.create_task(_do_connect(lec))

        # Планируем отключение
        _scheduler.add_job(
            _do_disconnect,
            DateTrigger(run_date=end_dt),
            id=f"d_{lid}",
            args=[lec],
            replace_existing=True
        )

        _planned.add(lid)


async def _sync_tsu_schedules():
    users = db.get_users_for_schedule_sync()
    if not users:
        return

    total_created = 0
    total_updated = 0
    total_deactivated = 0

    for user in users:
        try:
            result = await asyncio.to_thread(_sync_user_schedule_blocking, user)
        except Exception as exc:
            log.warning(
                "Не удалось синхронизировать расписание для %s (%s): %s",
                user["telegram_id"], user.get("group_name"), exc
            )
            continue

        for lecture_id in result["affected_ids"]:
            unplan(lecture_id)
        for lecture_id in result["stale_ids"]:
            unplan(lecture_id)

        total_created += result["created"]
        total_updated += result["updated"]
        total_deactivated += result["deactivated"]

    if total_created or total_updated or total_deactivated:
        log.info(
            "TSU sync: created=%d updated=%d deactivated=%d",
            total_created, total_updated, total_deactivated,
        )
    await _check_and_plan()


async def sync_user_now(telegram_id: int) -> dict:
    user = db.get_user(telegram_id)
    if not user or not user.get("group_id"):
        return {
            "ok": False,
            "error": "Сначала укажи группу.",
            "created": 0,
            "updated": 0,
            "deactivated": 0,
            "found": 0,
            "missing_url": 0,
            "skipped": 0,
        }

    try:
        result = await asyncio.to_thread(_sync_user_schedule_blocking, user)
    except Exception as exc:
        log.warning("Ручная синхронизация расписания не удалась: %s", exc)
        return {
            "ok": False,
            "error": str(exc),
            "created": 0,
            "updated": 0,
            "deactivated": 0,
            "found": 0,
            "missing_url": 0,
            "skipped": 0,
        }

    for lecture_id in result["affected_ids"]:
        unplan(lecture_id)
    for lecture_id in result["stale_ids"]:
        unplan(lecture_id)
    await _check_and_plan()
    return {"ok": True, **result}


def _sync_user_schedule_blocking(user: dict) -> dict:
    today = datetime.now(TZ).date()
    date_to = today + timedelta(days=SCHEDULE_LOOKAHEAD_DAYS)
    lessons = tsu_schedule.fetch_online_lessons(user["group_id"], today, date_to)
    group = group_presets.get_group(user.get("group_name")) or group_presets.get_group(user.get("group_id"))
    group_key = group["name"] if group else user.get("group_name")

    created = 0
    updated = 0
    missing_url = 0
    skipped = 0
    affected_ids: list[int] = []
    keep_external_ids: set[str] = set()

    for lesson in lessons:
        preset = group_presets.match_lecture(group_key, lesson.title, lesson.professor)
        if not preset:
            skipped += 1
            continue

        url = lesson.url
        if not url and preset.url:
            url = preset.url
        if not url:
            missing_url += 1
        lecture_id, is_created = db.upsert_schedule_lecture(
            telegram_id=user["telegram_id"],
            title=lesson.title,
            url=url,
            start_dt=lesson.start_dt,
            duration_min=lesson.duration_min,
            external_id=lesson.external_id,
            audience=lesson.audience,
            professor=lesson.professor,
        )
        affected_ids.append(lecture_id)
        keep_external_ids.add(lesson.external_id)
        if is_created:
            created += 1
        else:
            updated += 1

    since_start_dt = today.strftime("%Y-%m-%d 00:00")
    stale_ids = db.get_stale_schedule_lecture_ids(
        user["telegram_id"],
        keep_external_ids,
        since_start_dt,
    )
    deactivated = db.deactivate_missing_schedule_lectures(
        user["telegram_id"],
        keep_external_ids,
        since_start_dt,
    )

    return {
        "found": len(lessons),
        "missing_url": missing_url,
        "skipped": skipped,
        "created": created,
        "updated": updated,
        "deactivated": deactivated,
        "affected_ids": affected_ids,
        "stale_ids": stale_ids,
    }


async def _do_connect(lec: dict):
    """Подключает пользователя к лекции (вызывается планировщиком)"""
    uid  = lec["telegram_id"]
    name = lec["full_name"]

    if not (lec.get("url") or "").strip():
        await _do_missing_link_alert(lec)
        return

    log.info("🔔 Подключаем %s к '%s'", uid, lec["title"])
    ok = await browser.connect(uid, lec["url"], name)

    if _bot:
        text = (
            f"✅ Подключился к лекции!\n\n"
            f"📚 {lec['title']}\n"
            f"⏱ Выйду через {lec['duration_min']} мин."
        ) if ok else (
            f"❌ Не удалось подключиться к «{lec['title']}»\n"
            f"Проверь ближайшие занятия: /lectures"
        )
        try:
            await _bot.send_message(uid, text)
        except Exception:
            pass


async def _do_disconnect(lec: dict):
    """Отключает пользователя (вызывается планировщиком)"""
    uid = lec["telegram_id"]
    await browser.disconnect(uid)
    db.deactivate(lec["id"])
    _planned.discard(lec["id"])

    if _bot:
        try:
            await _bot.send_message(uid, f"👋 Вышел с лекции «{lec['title']}»")
        except Exception:
            pass


async def _do_missing_link_alert(lec: dict):
    uid = lec["telegram_id"]
    if not _bot:
        return
    try:
        await _bot.send_message(
            uid,
            "🔗 Для онлайн-пары нужна ссылка.\n\n"
            f"📚 {lec['title']}\n"
            f"🕐 {lec['start_dt']}\n\n"
            "Добавь ссылку для этого предмета в group_presets.py и обнови расписание."
        )
    except Exception:
        pass


async def _finish_missing_link_lecture(lec: dict):
    db.deactivate(lec["id"])
    _planned.discard(lec["id"])


async def _on_chat_threshold_disconnect(telegram_id: int, message_count: int):
    if not _bot:
        return
    try:
        await _bot.send_message(
            telegram_id,
            f"В чате уже {message_count} сообщений. Я вышел из лекции досрочно.",
        )
    except Exception:
        pass


def plan_now(lec: dict):
    """
    Немедленно планирует задачи для лекции.
    Вызывается из бота когда пользователь только что добавил лекцию.
    """
    asyncio.create_task(_check_and_plan())


def unplan(lecture_id: int):
    _planned.discard(lecture_id)
    if not _scheduler:
        return
    for job_id in (f"c_{lecture_id}", f"d_{lecture_id}", f"m_{lecture_id}"):
        try:
            _scheduler.remove_job(job_id)
        except Exception:
            pass
