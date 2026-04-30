"""
Telegram-бот для автоматического подключения к онлайн-лекциям ТГУ.

Сценарий:
    /start -> пользователь вводит ФИО -> выбирает группу
    поддерживаемые группы: 932401, 932402, 932403, 932404
    поддерживаемые предметы фиксированы в group_presets.py
    расписание берется из TSU.InTime, ссылки на e-class хранятся в group_presets.py
"""

import html
import logging
import sys
from datetime import datetime, timedelta

import pytz
from telegram import MenuButtonDefault, ReplyKeyboardMarkup, ReplyKeyboardRemove, Update
from telegram.ext import (
    Application,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

import browser
import database as db
import group_presets
import scheduler as sched
from config import BOT_TOKEN, TIMEZONE

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)

TZ = pytz.timezone(TIMEZONE)
ASK_NAME, ASK_GROUP, MAIN = range(3)

SUPPORTED_GROUP_KEYS = group_presets.group_keys()

MAIN_KB = ReplyKeyboardMarkup(
    [
        ["🔄 Обновить расписание", "📋 Мои лекции"],
        ["🎓 Группа", "📊 Статус"],
        ["👤 Изменить ФИО"],
    ],
    resize_keyboard=True,
)


def group_kb(cancel: bool = True) -> ReplyKeyboardMarkup:
    rows = [
        ["932401", "932402"],
        ["932403", "932404"],
    ]
    if cancel:
        rows.append(["🔙 Отмена"])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def back_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup([["🔙 Отмена"]], resize_keyboard=True)


async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    user = db.get_user(uid)

    if user:
        group = _user_group(user)
        if not group:
            await _ask_group(update, "Выбери свою группу:")
            return ASK_GROUP

        await update.message.reply_text(
            f"👋 Привет, <b>{html.escape(user['full_name'])}</b>!\n\n"
            f"Группа: <b>{html.escape(group['name'])}</b>\n"
            f"Предметы:\n{_preset_list_html(group['name'])}\n\n"
            "Бот сам обновляет расписание TSU.InTime и подключается к найденным занятиям.",
            parse_mode="HTML",
            reply_markup=MAIN_KB,
        )
        return MAIN

    ctx.user_data["after_name"] = "ask_group"
    await update.message.reply_text(
        "🎓 <b>TSU Lecture Bot</b>\n\n"
        "Поддерживаемые группы: <b>932401, 932402, 932403, 932404</b>.\n"
        "После выбора группы бот будет подтягивать расписание онлайн-лекций TSU.InTime.\n\n"
        "Введи своё <b>ФИО</b>, которое будет использоваться при входе гостем:\n"
        "<code>Кияев Владимир Сергеевич</code>",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardRemove(),
    )
    return ASK_NAME


async def got_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    name = update.message.text.strip()

    if len(name.split()) < 2:
        await update.message.reply_text(
            "Введи минимум фамилию и имя.\n"
            "Пример: <code>Кияев Владимир Сергеевич</code>",
            parse_mode="HTML",
        )
        return ASK_NAME

    db.save_user(uid, name)
    after_name = ctx.user_data.pop("after_name", "main")
    if after_name == "ask_group" or not _current_group(uid):
        await update.message.reply_text(
            f"✅ ФИО сохранено: <b>{html.escape(name)}</b>\n\n"
            "Теперь выбери свою группу:",
            parse_mode="HTML",
            reply_markup=group_kb(cancel=False),
        )
        return ASK_GROUP

    await update.message.reply_text(
        f"✅ ФИО обновлено: <b>{html.escape(name)}</b>",
        parse_mode="HTML",
        reply_markup=MAIN_KB,
    )
    return MAIN


async def change_name_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data["after_name"] = "main"
    await update.message.reply_text(
        "Введи новое <b>ФИО</b>:",
        parse_mode="HTML",
        reply_markup=back_kb(),
    )
    return ASK_NAME


async def group_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    group = _current_group(update.effective_user.id)
    prefix = f"Сейчас выбрана группа <b>{html.escape(group['name'])}</b>.\n\n" if group else ""
    await update.message.reply_text(
        prefix + "Доступны группы: <b>932401, 932402, 932403, 932404</b>.\nВыбери нужную:",
        parse_mode="HTML",
        reply_markup=group_kb(),
    )
    return ASK_GROUP


async def got_group(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    key = update.message.text.strip()
    group = group_presets.get_group(key)
    if not group or group["name"] not in SUPPORTED_GROUP_KEYS:
        await update.message.reply_text(
            "Пока доступны только группы: <b>932401, 932402, 932403, 932404</b>.\n"
            "Остальные добавим позже.",
            parse_mode="HTML",
            reply_markup=group_kb(),
        )
        return ASK_GROUP

    previous_group = _current_group(uid)
    if previous_group and previous_group["name"] != group["name"]:
        db.deactivate_user_schedule_lectures(uid)
    db.set_user_group(uid, group_presets.get_group_match(group["name"]))
    await update.message.reply_text(
        f"✅ Группа сохранена: <b>{html.escape(group['name'])}</b>\n\n"
        f"Предметы:\n{_preset_list_html(group['name'])}\n\n"
        "Обновляю расписание TSU.InTime...",
        parse_mode="HTML",
        reply_markup=MAIN_KB,
    )
    await _sync_and_reply(update, uid)
    return MAIN


async def sync_schedule(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    if not db.get_user(uid):
        await update.message.reply_text("Сначала введи ФИО через /start.")
        return MAIN
    if not _current_group(uid):
        await _ask_group(update, "Сначала выбери группу:")
        return ASK_GROUP

    await update.message.reply_text("Обновляю расписание TSU.InTime...")
    await _sync_and_reply(update, uid)
    return MAIN


async def my_lectures(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    group = _current_group(uid)
    if not group:
        await _ask_group(update, "Сначала выбери группу:")
        return ASK_GROUP

    lectures = _target_lectures(uid)
    if not lectures:
        await update.message.reply_text(
            "Активных занятий по поддерживаемым предметам пока нет.\n"
            "Нажми 🔄 Обновить расписание, чтобы подтянуть ближайшие пары.",
            reply_markup=MAIN_KB,
        )
        return MAIN

    now = datetime.now(TZ)
    lines = [f"📋 <b>Активные занятия группы {html.escape(group['name'])}</b>\n"]
    for lec in lectures[:12]:
        start = TZ.localize(datetime.strptime(lec["start_dt"], "%Y-%m-%d %H:%M"))
        end = start + timedelta(minutes=lec["duration_min"])
        if start <= now <= end:
            status = "🟢 сейчас"
        elif start > now:
            diff = start - now
            hours = int(diff.total_seconds() // 3600)
            minutes = int((diff.total_seconds() % 3600) // 60)
            status = f"⏰ через {hours}ч {minutes}м" if hours else f"⏰ через {minutes} мин"
        else:
            status = "завершена"

        details = []
        if lec.get("professor"):
            details.append(html.escape(lec["professor"]))
        if lec.get("audience"):
            details.append(html.escape(lec["audience"]))
        detail_text = "\n".join(f"  {item}" for item in details)
        if detail_text:
            detail_text = "\n" + detail_text

        lines.append(
            f"<b>{html.escape(lec['title'])}</b>\n"
            f"  {start.strftime('%d.%m %H:%M')}  |  {status}\n"
            f"  длительность: {lec['duration_min']} мин{detail_text}"
        )

    await update.message.reply_text(
        "\n\n".join(lines),
        parse_mode="HTML",
        reply_markup=MAIN_KB,
    )
    return MAIN


async def status(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = update.effective_user.id
    user = db.get_user(uid)
    if not user:
        await update.message.reply_text("Сначала введи ФИО через /start.")
        return MAIN

    group = _current_group(uid)
    if not group:
        await _ask_group(update, "Сначала выбери группу:")
        return ASK_GROUP

    lectures = _target_lectures(uid)
    next_lecture = _next_lecture_summary(lectures)
    connection = "в лекции" if browser.is_connected(uid) else "не подключён"

    if next_lecture:
        next_text = (
            f"<b>{html.escape(next_lecture['title'])}</b>\n"
            f"{next_lecture['starts_at']} · {next_lecture['duration_min']} мин"
        )
    else:
        next_text = "не найдена. Нажми «🔄 Обновить расписание»."

    await update.message.reply_text(
        "📊 <b>Статус</b>\n\n"
        f"👤 {html.escape(user['full_name'])}\n"
        f"🎓 {html.escape(group['name'])}\n"
        f"🔌 Сейчас: <b>{connection}</b>\n\n"
        f"📅 <b>Ближайшая пара</b>\n{next_text}\n\n"
        f"Найдено занятий: <b>{len(lectures)}</b>",
        parse_mode="HTML",
        reply_markup=MAIN_KB,
    )
    return MAIN


async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    ctx.user_data.clear()
    await update.message.reply_text("Отменено.", reply_markup=MAIN_KB)
    return MAIN


async def unknown(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "Используй кнопки меню. Ручное добавление лекций отключено.",
        reply_markup=MAIN_KB,
    )
    return MAIN


async def on_start(app: Application):
    db.init_db()
    try:
        await app.bot.set_chat_menu_button(menu_button=MenuButtonDefault())
    except Exception as exc:
        log.warning("Не удалось сбросить кнопку меню Telegram: %s", exc)
    await browser.start()
    sched.start(app.bot)
    log.info("Бот готов")


async def on_stop(app: Application):
    sched.stop()
    await browser.stop()


async def _sync_and_reply(update: Update, uid: int):
    result = await sched.sync_user_now(uid)
    if not result.get("ok"):
        await update.message.reply_text(
            "Не удалось обновить расписание:\n"
            f"<code>{html.escape(result.get('error', 'unknown error'))}</code>",
            parse_mode="HTML",
            reply_markup=MAIN_KB,
        )
        return

    active_count = len(_target_lectures(uid))
    matched = result.get("created", 0) + result.get("updated", 0)
    text = (
        "✅ Расписание обновлено.\n\n"
        f"Онлайн-пар в TSU.InTime: {result.get('found', 0)}\n"
        f"Поддерживаемых занятий обработано: {matched}\n"
        f"Добавлено: {result.get('created', 0)}\n"
        f"Обновлено: {result.get('updated', 0)}\n"
        f"Пропущено других онлайн-пар: {result.get('skipped', 0)}\n"
        f"Активных подключений: {active_count}"
    )
    if active_count == 0:
        text += "\n\nВ ближайшем окне расписания поддерживаемых лекций не найдено."

    await update.message.reply_text(text, reply_markup=MAIN_KB)


async def _ask_group(update: Update, text: str):
    await update.message.reply_text(
        text,
        parse_mode="HTML",
        reply_markup=group_kb(cancel=False),
    )


def _current_group(telegram_id: int) -> dict | None:
    user = db.get_user(telegram_id)
    if not user:
        return None
    return _user_group(user)


def _user_group(user: dict) -> dict | None:
    return (
        group_presets.get_group(user.get("group_name"))
        or group_presets.get_group(user.get("group_id"))
    )


def _target_lectures(telegram_id: int) -> list[dict]:
    group = _current_group(telegram_id)
    if not group:
        return []
    lectures = db.get_lectures(telegram_id, only_active=True)
    return [
        lec for lec in lectures
        if lec.get("source") == "tsu"
        and group_presets.match_lecture(group["name"], lec["title"], lec.get("professor") or "")
    ]


def _preset_list_html(group_key: str) -> str:
    presets = group_presets.list_lecture_presets(group_key)
    return "\n".join(f"• <b>{html.escape(preset.title)}</b>" for preset in presets)


def _next_lecture_summary(lectures: list[dict]) -> dict | None:
    now = datetime.now(TZ)
    for lec in lectures:
        start = TZ.localize(datetime.strptime(lec["start_dt"], "%Y-%m-%d %H:%M"))
        if start >= now:
            return {
                "title": lec["title"],
                "starts_at": start.strftime("%d.%m в %H:%M"),
                "duration_min": lec["duration_min"],
            }
    return None


def main():
    if not BOT_TOKEN:
        print("Нет BOT_TOKEN в .env файле")
        sys.exit(1)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(on_start)
        .post_shutdown(on_stop)
        .build()
    )

    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start)],
        states={
            ASK_NAME: [
                MessageHandler(filters.Regex("^🔙 Отмена$"), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, got_name),
            ],
            ASK_GROUP: [
                MessageHandler(filters.Regex("^🔙 Отмена$"), cancel),
                MessageHandler(filters.TEXT & ~filters.COMMAND, got_group),
            ],
            MAIN: [
                MessageHandler(filters.Regex("^🔄 Обновить расписание$"), sync_schedule),
                MessageHandler(filters.Regex("^📋 Мои лекции$"), my_lectures),
                MessageHandler(filters.Regex("^🎓 Группа$"), group_start),
                MessageHandler(filters.Regex("^📊 Статус$"), status),
                MessageHandler(filters.Regex("^👤 Изменить ФИО$"), change_name_start),
                CommandHandler("sync", sync_schedule),
                CommandHandler("lectures", my_lectures),
                CommandHandler("group", group_start),
                CommandHandler("status", status),
                MessageHandler(filters.TEXT & ~filters.COMMAND, unknown),
            ],
        },
        fallbacks=[
            CommandHandler("start", cmd_start),
            MessageHandler(filters.Regex("^🔙 Отмена$"), cancel),
        ],
        per_user=True,
        per_chat=True,
    )

    app.add_handler(conv)
    log.info("Запускаю бота")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)


if __name__ == "__main__":
    main()
