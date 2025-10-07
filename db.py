#Управление БД

import json
import time
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite

from config import DB_PATH

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS profiles (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    name TEXT,
    age INTEGER,
    city TEXT,
    gender TEXT, -- 'M' or 'F'
    looking_for TEXT, -- 'M' 'F' 'ANY'
    description TEXT,
    photo_file_id TEXT,
    embedding TEXT, -- JSON of floats
    updated_at INTEGER
);

CREATE TABLE IF NOT EXISTS interactions (
    user_id INTEGER,
    target_id INTEGER,
    action TEXT, -- 'like' or 'dislike'
    ts INTEGER,
    PRIMARY KEY (user_id, target_id)
);

CREATE TABLE IF NOT EXISTS virtual_chats (
    user_id INTEGER PRIMARY KEY,
    partner_gender TEXT, -- 'M' or 'F'
    history TEXT, -- JSON list [{role, content}]
    updated_at INTEGER
);
"""

def now_ts() -> int:
    return int(time.time())

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_TABLES_SQL)
        await db.commit()

async def get_profile(user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM profiles WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
    return dict(row) if row else None

async def upsert_profile(user_id: int, **kwargs) -> None:
    existing = await get_profile(user_id)
    fields = [
        "username",
        "name",
        "age",
        "city",
        "gender",
        "looking_for",
        "description",
        "photo_file_id",
        "embedding",
        "updated_at",
    ]
    values = {k: kwargs.get(k, (existing or {}).get(k)) for k in fields}
    values["updated_at"] = now_ts()

    async with aiosqlite.connect(DB_PATH) as db:
        if existing:
            await db.execute(
                """
                UPDATE profiles SET
                  username = ?, name = ?, age = ?, city = ?, gender = ?,
                  looking_for = ?, description = ?, photo_file_id = ?,
                  embedding = ?, updated_at = ?
                WHERE user_id = ?
                """,
                (
                    values["username"],
                    values["name"],
                    values["age"],
                    values["city"],
                    values["gender"],
                    values["looking_for"],
                    values["description"],
                    values["photo_file_id"],
                    json.dumps(values["embedding"]) if isinstance(values["embedding"], list) else values["embedding"],
                    values["updated_at"],
                    user_id,
                ),
            )
        else:
            await db.execute(
                """
                INSERT INTO profiles (user_id, username, name, age, city, gender, looking_for, description, photo_file_id, embedding, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_id,
                    values["username"],
                    values["name"],
                    values["age"],
                    values["city"],
                    values["gender"],
                    values["looking_for"],
                    values["description"],
                    values["photo_file_id"],
                    json.dumps(values["embedding"]) if isinstance(values["embedding"], list) else values["embedding"],
                    values["updated_at"],
                ),
            )
        await db.commit()

async def record_interaction(user_id: int, target_id: int, action: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO interactions (user_id, target_id, action, ts) VALUES (?, ?, ?, ?)",
            (user_id, target_id, action, now_ts()),
        )
        await db.commit()

async def has_interaction(user_id: int, target_id: int, action: Optional[str] = None) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        if action:
            cur = await db.execute(
                "SELECT 1 FROM interactions WHERE user_id = ? AND target_id = ? AND action = ?",
                (user_id, target_id, action),
            )
        else:
            cur = await db.execute(
                "SELECT 1 FROM interactions WHERE user_id = ? AND target_id = ?",
                (user_id, target_id),
            )
        row = await cur.fetchone()
        return row is not None

async def count_pending_likers(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT COUNT(*) FROM (
                SELECT i.user_id
                FROM interactions i
                WHERE i.target_id = ?
                  AND i.action = 'like'
                  AND NOT EXISTS (
                        SELECT 1 FROM interactions x
                        WHERE x.user_id = ?
                          AND x.target_id = i.user_id
                    )
                GROUP BY i.user_id
            )
            """,
            (user_id, user_id),
        )
        row = await cur.fetchone()
        return int(row[0]) if row else 0

async def get_next_pending_liker(user_id: int) -> Optional[Dict[str, Any]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT i.user_id AS liker_id
            FROM interactions i
            WHERE i.target_id = ?
              AND i.action = 'like'
              AND NOT EXISTS (
                    SELECT 1 FROM interactions x
                    WHERE x.user_id = ?
                      AND x.target_id = i.user_id
                )
            ORDER BY i.ts ASC
            LIMIT 1
            """,
            (user_id, user_id),
        )
        row = await cur.fetchone()
        if not row:
            return None
        liker_id = row["liker_id"]
    liker_profile = await get_profile(liker_id)
    return liker_profile

async def get_virtual_state(user_id: int) -> Tuple[Optional[str], List[Dict[str, str]]]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT partner_gender, history FROM virtual_chats WHERE user_id = ?", (user_id,))
        row = await cur.fetchone()
    if not row:
        return None, []
    partner_gender = row["partner_gender"]
    try:
        history = json.loads(row["history"]) if row["history"] else []
    except Exception:
        history = []
    return partner_gender, history

async def set_virtual_state(user_id: int, partner_gender: Optional[str], history: List[Dict[str, str]]):
    async with aiosqlite.connect(DB_PATH) as db:
        if partner_gender is None and not history:
            await db.execute("DELETE FROM virtual_chats WHERE user_id = ?", (user_id,))
        else:
            await db.execute(
                "INSERT OR REPLACE INTO virtual_chats (user_id, partner_gender, history, updated_at) VALUES (?, ?, ?, ?)",
                (user_id, partner_gender, json.dumps(history, ensure_ascii=False), now_ts()),
            )

        await db.commit()
