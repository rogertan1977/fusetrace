"""FuseTrace - SQLite 儲存層（純標準庫 sqlite3，WAL 模式）。

Tables
------
liquidations    : 原始爆倉紀錄（可關閉；供之後的起火點歸因與 replay 使用）
events          : 偵測到的 Cascade 事件摘要
event_timeline  : 事件時間軸（每筆爆倉一列）
"""
from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Iterable, Sequence

SCHEMA = """
CREATE TABLE IF NOT EXISTS liquidations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL    NOT NULL,          -- 交易所事件時間 (unix 秒)
    symbol       TEXT    NOT NULL,
    side         TEXT    NOT NULL,          -- LONG = 多單被爆, SHORT = 空單被爆
    price        REAL,
    qty          REAL,
    notional_usd REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_liq_ts ON liquidations(ts);

CREATE TABLE IF NOT EXISTS events (
    id                TEXT PRIMARY KEY,     -- 例如 20260101-140320
    started_at        REAL NOT NULL,
    ended_at          REAL,
    status            TEXT NOT NULL,        -- open | closed | interrupted
    trigger_total_usd REAL,                 -- 觸發當下視窗內總爆倉額
    peak_window_usd   REAL,                 -- 事件期間視窗峰值
    total_usd         REAL,
    long_usd          REAL,
    short_usd         REAL,
    n_liquidations    INTEGER,
    n_symbols         INTEGER,
    first_symbol      TEXT,                 -- 事件中最早出現爆倉的幣種（歸因的雛形）
    first_ts          REAL,
    top_symbols       TEXT                  -- JSON: [[symbol, usd], ...]
);
CREATE INDEX IF NOT EXISTS idx_events_started ON events(started_at);

CREATE TABLE IF NOT EXISTS event_timeline (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id     TEXT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    ts           REAL NOT NULL,
    symbol       TEXT NOT NULL,
    side         TEXT NOT NULL,
    price        REAL,
    notional_usd REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_tl_event ON event_timeline(event_id, ts);

-- 前兆證據：每個候選起火點一列（價格 / OI / 資金費率 + 啟發式分數）
CREATE TABLE IF NOT EXISTS event_precursors (
    event_id       TEXT    NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    symbol         TEXT    NOT NULL,
    rank           INTEGER NOT NULL,
    is_ignition    INTEGER NOT NULL DEFAULT 0,   -- 1 = 被選為「疑似起火點」
    score          REAL,
    notional_usd   REAL,                         -- 觸發當下視窗內該幣種爆倉額
    first_seen_ts  REAL,                         -- 該幣種在視窗內最早出現爆倉的時間
    ref_ts         REAL,                         -- 回溯基準時間（事件第一筆爆倉）
    price_pct      REAL,                         -- 回溯期間價格變化 %
    price_span_min REAL,
    oi_pct         REAL,                         -- 回溯期間 OI（幣本位數量）變化 %
    oi_span_min    REAL,
    funding_pct    REAL,                         -- 資金費率（%），例如 0.01 = 0.01%
    n_evidence     INTEGER,                      -- 取得幾項證據（0~3）
    errors         TEXT,                         -- JSON: 取數失敗原因
    PRIMARY KEY (event_id, symbol)
);
"""


PRECURSOR_COLS = (
    "event_id", "symbol", "rank", "is_ignition", "score", "notional_usd", "first_seen_ts",
    "ref_ts", "price_pct", "price_span_min", "oi_pct", "oi_span_min", "funding_pct",
    "n_evidence", "errors",
)


class Database:
    def __init__(self, path: str = "fusetrace.db") -> None:
        p = Path(path)
        if p.parent != Path("."):
            p.parent.mkdir(parents=True, exist_ok=True)
        self.path = str(p)
        self.conn = sqlite3.connect(self.path, timeout=10)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")   # WAL 下兼顧效能與安全
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.conn.executescript(SCHEMA)
        self.conn.commit()

    # ---------------------------------------------------------------- writes
    def insert_liquidations(self, rows: Iterable[Sequence]) -> None:
        """rows: (ts, symbol, side, price, qty, notional_usd)"""
        with self.conn:
            self.conn.executemany(
                "INSERT INTO liquidations(ts, symbol, side, price, qty, notional_usd) "
                "VALUES (?,?,?,?,?,?)",
                rows,
            )

    def upsert_event(self, row: dict) -> None:
        """新增或更新事件摘要（row 的 key 即欄位名，必須含 id）。"""
        cols = list(row)
        sql = (
            f"INSERT INTO events({','.join(cols)}) "
            f"VALUES({','.join(':' + c for c in cols)}) "
            f"ON CONFLICT(id) DO UPDATE SET "
            f"{','.join(f'{c}=excluded.{c}' for c in cols if c != 'id')}"
        )
        with self.conn:
            self.conn.execute(sql, row)

    def add_timeline(self, rows: Iterable[Sequence]) -> None:
        """rows: (event_id, ts, symbol, side, price, notional_usd)"""
        with self.conn:
            self.conn.executemany(
                "INSERT INTO event_timeline(event_id, ts, symbol, side, price, notional_usd) "
                "VALUES (?,?,?,?,?,?)",
                rows,
            )

    def save_precursors(self, event_id: str, rows: list[dict]) -> None:
        """rows 每列需含 PRECURSOR_COLS 中除 event_id 外的全部欄位。重複呼叫會覆蓋舊資料。"""
        cols = PRECURSOR_COLS
        sql = (f"INSERT INTO event_precursors({','.join(cols)}) "
               f"VALUES({','.join(':' + c for c in cols)})")
        with self.conn:
            self.conn.execute("DELETE FROM event_precursors WHERE event_id=?", (event_id,))
            self.conn.executemany(sql, [{**r, "event_id": event_id} for r in rows])

    # ----------------------------------------------------------- maintenance
    def recover_open_events(self) -> int:
        """程式上次異常中斷時，把殘留的 open 事件標記為 interrupted。"""
        with self.conn:
            cur = self.conn.execute(
                "UPDATE events SET status='interrupted' WHERE status='open'"
            )
        return cur.rowcount

    def purge(self, retention_days: float) -> None:
        if retention_days <= 0:
            return
        cutoff = time.time() - retention_days * 86400
        with self.conn:
            self.conn.execute("DELETE FROM liquidations WHERE ts < ?", (cutoff,))
            self.conn.execute(
                "DELETE FROM events WHERE COALESCE(ended_at, started_at) < ?", (cutoff,)
            )  # event_timeline 透過 ON DELETE CASCADE 一併清除
        self.conn.execute("PRAGMA wal_checkpoint(PASSIVE)")

    # ----------------------------------------------------------------- reads
    def list_events(self, limit: int = 20) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM events ORDER BY started_at DESC LIMIT ?", (limit,)
        ).fetchall()

    def get_precursors(self, event_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM event_precursors WHERE event_id=? ORDER BY rank", (event_id,)
        ).fetchall()

    def get_timeline(self, event_id: str) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM event_timeline WHERE event_id=? ORDER BY ts", (event_id,)
        ).fetchall()

    def close(self) -> None:
        try:
            self.conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            self.conn.close()
