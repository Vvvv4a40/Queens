import sqlite3
import logging
from config import DATABASE
from tsu_schedule import GroupMatch

log = logging.getLogger(__name__)


def get_conn():
    c = sqlite3.connect(DATABASE)
    c.row_factory = sqlite3.Row   # row["field"] вместо row[0]
    return c


def init_db():
    """Создаёт таблицы при первом запуске"""
    with get_conn() as c:
        # Таблица пользователей — только ФИО
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                telegram_id  INTEGER PRIMARY KEY,
                full_name    TEXT NOT NULL,
                group_id     TEXT,
                group_name   TEXT,
                faculty_id   TEXT,
                schedule_enabled INTEGER DEFAULT 1
            )
        """)

        # Таблица лекций
        # start_dt  — строка формата "2026-03-15 08:45" (томское время)
        # duration  — сколько минут сидеть на лекции
        # active    — 1 пока не прошла, 0 после выхода
        c.execute("""
            CREATE TABLE IF NOT EXISTS lectures (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id  INTEGER NOT NULL,
                title        TEXT NOT NULL,
                url          TEXT NOT NULL,
                start_dt     TEXT NOT NULL,
                duration_min INTEGER NOT NULL,
                active       INTEGER DEFAULT 1,
                source       TEXT DEFAULT 'tsu',
                external_id  TEXT,
                audience     TEXT,
                professor    TEXT,
                FOREIGN KEY(telegram_id) REFERENCES users(telegram_id)
            )
        """)
        _ensure_column(c, "users", "group_id", "TEXT")
        _ensure_column(c, "users", "group_name", "TEXT")
        _ensure_column(c, "users", "faculty_id", "TEXT")
        _ensure_column(c, "users", "schedule_enabled", "INTEGER DEFAULT 1")
        _ensure_column(c, "lectures", "source", "TEXT DEFAULT 'tsu'")
        _ensure_column(c, "lectures", "external_id", "TEXT")
        _ensure_column(c, "lectures", "audience", "TEXT")
        _ensure_column(c, "lectures", "professor", "TEXT")
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_lectures_schedule_source
            ON lectures (telegram_id, source, external_id)
        """)
        c.execute("""
            CREATE INDEX IF NOT EXISTS idx_lectures_missing_url
            ON lectures (telegram_id, source, active, start_dt)
        """)
        c.commit()
    log.info("БД готова: %s", DATABASE)


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, declaration: str):
    existing = {
        row["name"]
        for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {declaration}")


# ── Пользователи ────────────────────────────────────────────

def save_user(telegram_id: int, full_name: str):
    with get_conn() as c:
        c.execute("""
            INSERT INTO users (telegram_id, full_name) VALUES (?, ?)
            ON CONFLICT(telegram_id) DO UPDATE SET full_name = excluded.full_name
        """, (telegram_id, full_name))
        c.commit()


def get_user(telegram_id: int) -> dict | None:
    with get_conn() as c:
        row = c.execute(
            "SELECT * FROM users WHERE telegram_id = ?", (telegram_id,)
        ).fetchone()
    return dict(row) if row else None


def get_name(telegram_id: int) -> str | None:
    u = get_user(telegram_id)
    return u["full_name"] if u else None


def set_user_group(telegram_id: int, group: GroupMatch):
    with get_conn() as c:
        c.execute("""
            UPDATE users
            SET group_id = ?, group_name = ?, faculty_id = ?, schedule_enabled = 1
            WHERE telegram_id = ?
        """, (group.id, group.name, group.faculty_id, telegram_id))
        c.commit()


def get_users_for_schedule_sync() -> list[dict]:
    with get_conn() as c:
        rows = c.execute("""
            SELECT *
            FROM users
            WHERE group_id IS NOT NULL
              AND group_id != ''
              AND schedule_enabled = 1
        """).fetchall()
    return [dict(r) for r in rows]


# ── Лекции ──────────────────────────────────────────────────

def upsert_schedule_lecture(
    telegram_id: int,
    title: str,
    url: str,
    start_dt: str,
    duration_min: int,
    external_id: str,
    audience: str = "",
    professor: str = "",
) -> tuple[int, bool]:
    with get_conn() as c:
        row = c.execute("""
            SELECT id
            FROM lectures
            WHERE telegram_id = ? AND source = 'tsu' AND external_id = ?
        """, (telegram_id, external_id)).fetchone()
        if row:
            c.execute("""
                UPDATE lectures
                SET title = ?, url = ?, start_dt = ?, duration_min = ?,
                    active = 1, audience = ?, professor = ?
                WHERE id = ?
            """, (title, url, start_dt, duration_min, audience, professor, row["id"]))
            c.commit()
            return int(row["id"]), False

        cur = c.execute("""
            INSERT INTO lectures (
                telegram_id, title, url, start_dt, duration_min,
                active, source, external_id, audience, professor
            )
            VALUES (?, ?, ?, ?, ?, 1, 'tsu', ?, ?, ?)
        """, (
            telegram_id, title, url, start_dt, duration_min,
            external_id, audience, professor,
        ))
        c.commit()
        return int(cur.lastrowid), True


def deactivate_missing_schedule_lectures(
    telegram_id: int,
    keep_external_ids: set[str],
    since_start_dt: str,
) -> int:
    with get_conn() as c:
        if keep_external_ids:
            placeholders = ",".join("?" for _ in keep_external_ids)
            params = [telegram_id, since_start_dt, *keep_external_ids]
            cur = c.execute(f"""
                UPDATE lectures
                SET active = 0
                WHERE telegram_id = ?
                  AND source = 'tsu'
                  AND active = 1
                  AND start_dt >= ?
                  AND external_id NOT IN ({placeholders})
            """, params)
        else:
            cur = c.execute("""
                UPDATE lectures
                SET active = 0
                WHERE telegram_id = ?
                  AND source = 'tsu'
                  AND active = 1
                  AND start_dt >= ?
            """, (telegram_id, since_start_dt))
        c.commit()
        return int(cur.rowcount)


def deactivate_user_schedule_lectures(telegram_id: int) -> int:
    with get_conn() as c:
        cur = c.execute("""
            UPDATE lectures
            SET active = 0
            WHERE telegram_id = ?
              AND source = 'tsu'
              AND active = 1
        """, (telegram_id,))
        c.commit()
        return int(cur.rowcount)


def get_stale_schedule_lecture_ids(
    telegram_id: int,
    keep_external_ids: set[str],
    since_start_dt: str,
) -> list[int]:
    with get_conn() as c:
        if keep_external_ids:
            placeholders = ",".join("?" for _ in keep_external_ids)
            params = [telegram_id, since_start_dt, *keep_external_ids]
            rows = c.execute(f"""
                SELECT id
                FROM lectures
                WHERE telegram_id = ?
                  AND source = 'tsu'
                  AND active = 1
                  AND start_dt >= ?
                  AND external_id NOT IN ({placeholders})
            """, params).fetchall()
        else:
            rows = c.execute("""
                SELECT id
                FROM lectures
                WHERE telegram_id = ?
                  AND source = 'tsu'
                  AND active = 1
                  AND start_dt >= ?
            """, (telegram_id, since_start_dt)).fetchall()
    return [int(r["id"]) for r in rows]


def get_lectures(telegram_id: int, only_active: bool = False) -> list[dict]:
    """Все лекции пользователя (или только активные)"""
    with get_conn() as c:
        q = "SELECT * FROM lectures WHERE telegram_id = ?"
        if only_active:
            q += " AND active = 1"
        q += " ORDER BY start_dt"
        rows = c.execute(q, (telegram_id,)).fetchall()
    return [dict(r) for r in rows]


def get_all_active() -> list[dict]:
    """Активные лекции TSU.InTime всех пользователей (для планировщика)."""
    with get_conn() as c:
        rows = c.execute("""
            SELECT l.*, u.full_name
            FROM lectures l
            JOIN users u ON l.telegram_id = u.telegram_id
            WHERE l.active = 1
              AND l.source = 'tsu'
            ORDER BY l.start_dt
        """).fetchall()
    return [dict(r) for r in rows]


def deactivate(lecture_id: int):
    """Помечает лекцию как завершённую"""
    with get_conn() as c:
        c.execute("UPDATE lectures SET active = 0 WHERE id = ?", (lecture_id,))
        c.commit()
