#!/usr/bin/env python3
"""FuseTrace MVP - Binance Futures 連環爆倉（Liquidation Cascade）偵測器。

資料流：
  Binance WS (!forceOrder@arr) -> parse -> RollingWindow(60s) + Baseline(分鐘桶)
                                         -> CascadeDetector -> SQLite (WAL)
                                                            -> Telegram (背景線程, urllib)

用法：
  python fusetrace.py                       # 連線 Binance 即時監控
  python fusetrace.py --simulate            # 合成資料演練一次完整 Cascade（不需網路）
  python fusetrace.py --list-events         # 列出已記錄的事件
  python fusetrace.py --replay <event_id>   # 重播事件（可用 latest 或 id 前綴）
  python fusetrace.py --test-telegram       # 發送一則測試訊息，驗證 token / chat_id
  python fusetrace.py --probe ETHUSDT       # 對單一幣種試跑前兆取數，驗證 REST 端點可用
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import queue
import random
import re
import signal
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from collections import deque
from dataclasses import dataclass
from pathlib import Path

from database import Database

log = logging.getLogger("fusetrace")

# --------------------------------------------------------------------------
# 設定：優先使用 PyYAML（若已安裝），否則用內建的迷你解析器（僅支援兩層 key: value）
# --------------------------------------------------------------------------
DEFAULTS: dict = {
    "source": {
        # 2026-03 起 Binance 將 USDⓈ-M WS 拆成 /public /market /private；
        # !forceOrder@arr 屬於 /market，舊的 /ws/ 路徑已停用
        "ws_url": "wss://fstream.binance.com/market/ws/!forceOrder@arr",
        "rest_url": "https://fapi.binance.com",
    },
    "detection": {
        "window_seconds": 60,
        "min_total_usd": 10_000_000,
        "min_symbols": 3,
        "baseline_minutes": 60,
        "min_baseline_minutes": 10,
        "baseline_multiplier": 5.0,
        "end_ratio": 0.5,
        "quiet_seconds": 30,
        "max_timeline_rows": 5000,
    },
    "storage": {
        "path": "fusetrace.db",
        "retention_days": 30,
        "store_all_liquidations": True,
    },
    "telegram": {
        "enabled": False,
        "bot_token": "",
        "chat_id": "",
        "notify_on_open": True,
        "notify_on_close": True,
        "timeout_seconds": 10,
    },
    "precursor": {
        "enabled": True,
        "lookback_minutes": 15,
        "top_k": 3,
        "include_first_seen": True,
        "max_workers": 3,
        "request_timeout": 4,
        "wait_seconds": 4,
        "cooldown_seconds": 120,
        "backoff_seconds": 300,
        "min_score": 0.25,
        "price_scale_pct": 2.0,
        "oi_scale_pct": 5.0,
        "funding_scale_pct": 0.05,
        "weight_price": 0.5,
        "weight_oi": 0.3,
        "weight_funding": 0.2,
    },
    "logging": {"level": "INFO"},
}


def _strip_comment(line: str) -> str:
    quote = None
    for i, ch in enumerate(line):
        if quote:
            if ch == quote:
                quote = None
        elif ch in "\"'":
            quote = ch
        elif ch == "#" and (i == 0 or line[i - 1].isspace()):
            return line[:i]
    return line


def _scalar(v: str):
    v = v.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in "\"'":
        return v[1:-1]
    low = v.lower()
    if low in ("true", "yes", "on"):
        return True
    if low in ("false", "no", "off"):
        return False
    for cast in (int, float):
        try:
            return cast(v)  # 支援 10_000_000
        except ValueError:
            pass
    return v


def _mini_yaml(text: str) -> dict:
    root: dict = {}
    section = root
    for raw in text.splitlines():
        line = _strip_comment(raw).rstrip()
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip())
        key, _, val = line.strip().partition(":")
        key = key.strip()
        if indent == 0:
            if val.strip() == "":
                section = root.setdefault(key, {})
            else:
                root[key] = _scalar(val)
                section = root
        else:
            section[key] = _scalar(val)
    return root


def load_config(path: str) -> dict:
    cfg = {k: dict(v) for k, v in DEFAULTS.items()}
    p = Path(path)
    if not p.exists():
        print(f"[warn] {path} not found, using built-in defaults", file=sys.stderr)
        return cfg
    text = p.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        user = yaml.safe_load(text) or {}
    except ImportError:
        user = _mini_yaml(text)
    for section, values in user.items():
        if isinstance(values, dict):
            cfg.setdefault(section, {}).update(values)
    return cfg


# --------------------------------------------------------------------------
# 資料模型與解析
# --------------------------------------------------------------------------
@dataclass(slots=True)
class Liq:
    ts: float        # 交易所事件時間（寫入資料庫用）
    arrival: float   # 本機收到時間（滑動視窗過期用，避免時鐘偏移）
    symbol: str
    side: str        # LONG = 多單被爆, SHORT = 空單被爆
    price: float
    qty: float
    notional: float

    def row(self) -> tuple:
        return (self.ts, self.symbol, self.side, self.price, self.qty, self.notional)


def parse_force_order(msg: dict) -> Liq | None:
    if not isinstance(msg, dict) or msg.get("e") != "forceOrder":
        return None
    o = msg.get("o")
    if not o:
        return None
    price = float(o.get("ap") or 0) or float(o.get("p") or 0)   # 優先用成交均價
    qty = float(o.get("z") or 0) or float(o.get("q") or 0)      # 優先用已成交量
    notional = price * qty
    if notional <= 0:
        return None
    now = time.time()
    ts_ms = o.get("T") or msg.get("E")
    ts = ts_ms / 1000.0 if ts_ms else now
    # 強平單方向：SELL = 多單被強制平倉；BUY = 空單被強制平倉
    side = "LONG" if o.get("S") == "SELL" else "SHORT"
    return Liq(ts, now, o["s"], side, price, qty, notional)


# --------------------------------------------------------------------------
# 滑動視窗：deque + 增量累計，O(1) 新增 / 過期
# --------------------------------------------------------------------------
class RollingWindow:
    def __init__(self, seconds: float) -> None:
        self.seconds = seconds
        self.items: deque[Liq] = deque()
        self.by_symbol: dict[str, float] = {}
        self.total = 0.0
        self.long = 0.0
        self.short = 0.0

    def add(self, liq: Liq) -> None:
        self.items.append(liq)
        self.total += liq.notional
        if liq.side == "LONG":
            self.long += liq.notional
        else:
            self.short += liq.notional
        self.by_symbol[liq.symbol] = self.by_symbol.get(liq.symbol, 0.0) + liq.notional

    def expire(self, now: float) -> None:
        cutoff = now - self.seconds
        while self.items and self.items[0].arrival < cutoff:
            l = self.items.popleft()
            self.total -= l.notional
            if l.side == "LONG":
                self.long -= l.notional
            else:
                self.short -= l.notional
            left = self.by_symbol.get(l.symbol, 0.0) - l.notional
            if left <= 1e-6:
                self.by_symbol.pop(l.symbol, None)
            else:
                self.by_symbol[l.symbol] = left
        if not self.items:  # 清掉浮點累積誤差
            self.total = self.long = self.short = 0.0
            self.by_symbol.clear()

    @property
    def n_symbols(self) -> int:
        return len(self.by_symbol)


# --------------------------------------------------------------------------
# 基線：以「分鐘桶」記錄過去 N 分鐘的爆倉額（最多 baseline_minutes 個桶，很省記憶體）
# --------------------------------------------------------------------------
class Baseline:
    def __init__(self, minutes: int) -> None:
        self.minutes = minutes
        self.buckets: deque[list] = deque()  # [minute_idx, usd]
        self.sum = 0.0
        self.start_idx = int(time.time() // 60)

    def add(self, now: float, usd: float) -> None:
        idx = int(now // 60)
        if self.buckets and self.buckets[-1][0] == idx:
            self.buckets[-1][1] += usd
        else:
            self.buckets.append([idx, usd])
        self.sum += usd
        self._prune(idx)

    def _prune(self, cur: int) -> None:
        while self.buckets and self.buckets[0][0] < cur - self.minutes:
            self.sum -= self.buckets.popleft()[1]
        if not self.buckets:
            self.sum = 0.0

    def avg_per_minute(self, now: float, window_s: float, min_minutes: int) -> float | None:
        """排除最近一個視窗後的「每分鐘平均爆倉額」；樣本不足回傳 None（暖機中）。"""
        cur = int(now // 60)
        self._prune(cur)
        cutoff = cur - math.ceil(window_s / 60)
        recent = 0.0
        for idx, usd in reversed(self.buckets):
            if idx < cutoff:
                break
            recent += usd
        span = cutoff - max(self.start_idx, cur - self.minutes)  # 沒有桶的分鐘視為 0
        if span <= 0 or span < min_minutes:
            return None
        return max(self.sum - recent, 0.0) / span


# --------------------------------------------------------------------------
# Cascade 事件
# --------------------------------------------------------------------------
class Event:
    def __init__(self, eid: str, trigger_total: float, now: float, max_timeline: int) -> None:
        self.id = eid
        self.status = "open"
        self.started_at: float | None = None
        self.last_ts: float | None = None
        self.trigger_total = trigger_total
        self.peak = trigger_total
        self.last_hot = now
        self.total = self.long = self.short = 0.0
        self.n = 0
        self.by_symbol: dict[str, float] = {}
        self.first: tuple[str, float] | None = None
        self.pending: list[tuple] = []   # 尚未寫入 event_timeline 的列
        self.tl_count = 0
        self.max_tl = max_timeline

    def add(self, l: Liq) -> None:
        if self.first is None:
            self.first = (l.symbol, l.ts)
            self.started_at = l.ts
        self.last_ts = l.ts
        self.n += 1
        self.total += l.notional
        if l.side == "LONG":
            self.long += l.notional
        else:
            self.short += l.notional
        self.by_symbol[l.symbol] = self.by_symbol.get(l.symbol, 0.0) + l.notional
        if self.tl_count < self.max_tl:
            self.pending.append((self.id, l.ts, l.symbol, l.side, l.price, l.notional))
            self.tl_count += 1

    def top(self, k: int = 5) -> list[tuple[str, float]]:
        return sorted(self.by_symbol.items(), key=lambda kv: kv[1], reverse=True)[:k]

    def to_row(self) -> dict:
        return {
            "id": self.id,
            "started_at": self.started_at,
            "ended_at": self.last_ts,
            "status": self.status,
            "trigger_total_usd": self.trigger_total,
            "peak_window_usd": self.peak,
            "total_usd": self.total,
            "long_usd": self.long,
            "short_usd": self.short,
            "n_liquidations": self.n,
            "n_symbols": len(self.by_symbol),
            "first_symbol": self.first[0] if self.first else None,
            "first_ts": self.first[1] if self.first else None,
            "top_symbols": json.dumps([[s, round(v, 2)] for s, v in self.top()]),
        }


def fmt_usd(v: float) -> str:
    if v >= 1e9:
        return f"${v / 1e9:.2f}B"
    if v >= 1e6:
        return f"${v / 1e6:.2f}M"
    if v >= 1e3:
        return f"${v / 1e3:.1f}K"
    return f"${v:.0f}"


def fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.gmtime(ts)) + " UTC"


# --------------------------------------------------------------------------
# 前兆歸因（Precursor Attribution）
#
# 設計原則：平時 0 請求。只有 Cascade 觸發時，才對「候選幣種」（視窗內爆倉額前 K 名
# + 最早出現的幣種，至多約 4 個）各發 3 個公開 REST 請求，合計 ~12 個請求 / 每次事件：
#   GET /fapi/v1/klines               1m K 線  -> 事件前 N 分鐘價格變化
#   GET /futures/data/openInterestHist 5m 統計 -> 事件前 N 分鐘 OI 變化（文件標示權重 0）
#   GET /fapi/v1/premiumIndex          標記價與資金費率
# 全部為公開端點、免 API Key。含冷卻、併發上限、429/418 退避。
# --------------------------------------------------------------------------
class RestError(Exception):
    pass


class RestBlocked(RestError):
    """被限流（429/418）後的退避期間，不再發任何請求。"""


class BinanceREST:
    def __init__(self, base: str, timeout: float = 4.0, backoff: float = 300.0) -> None:
        self.base = base.rstrip("/")
        self.timeout = timeout
        self.backoff = backoff
        self.blocked_until = 0.0
        self.used_weight: str | None = None   # 最近一次回應的 X-MBX-USED-WEIGHT-1M
        self.n_requests = 0
        self._lock = threading.Lock()

    def get(self, path: str, params: dict):
        if time.time() < self.blocked_until:
            raise RestBlocked("REST paused after rate limit")
        url = f"{self.base}{path}?{urllib.parse.urlencode(params)}"
        req = urllib.request.Request(url, headers={"User-Agent": "fusetrace/0.3 (public data only)"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                with self._lock:
                    self.n_requests += 1
                    w = resp.headers.get("X-MBX-USED-WEIGHT-1M")
                    if w:
                        self.used_weight = w
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code in (418, 429):   # 429 = 限流警告，418 = 已被暫時封鎖
                try:
                    retry = float(e.headers.get("Retry-After") or 0)
                except ValueError:
                    retry = 0.0
                pause = max(retry, self.backoff)
                with self._lock:
                    self.blocked_until = time.time() + pause
                log.warning("Binance REST HTTP %s -> pausing precursor lookups for %.0fs", e.code, pause)
                raise RestBlocked(f"HTTP {e.code}") from None
            raise RestError(f"HTTP {e.code}") from None
        except (urllib.error.URLError, TimeoutError, OSError, ValueError) as e:
            raise RestError(type(e).__name__) from None


class PrecursorEngine:
    def __init__(self, rest: BinanceREST, lookback_min: int, max_workers: int) -> None:
        self.rest = rest
        self.lookback = lookback_min
        self.max_workers = max(1, max_workers)

    def collect(self, symbols: list[str], t_ref: float) -> dict[str, dict]:
        """在背景線程中呼叫（阻塞式）。回傳 {symbol: evidence}，任何失敗都只記進 errors。"""
        if not symbols:
            return {}
        with ThreadPoolExecutor(max_workers=min(self.max_workers, len(symbols))) as pool:
            return dict(zip(symbols, pool.map(lambda s: self._one(s, t_ref), symbols)))

    def _one(self, sym: str, t_ref: float) -> dict:
        ev: dict = {"price_pct": None, "price_span_min": None, "oi_pct": None,
                    "oi_span_min": None, "funding_pct": None, "errors": []}
        t0 = t_ref - self.lookback * 60
        for name, fn in (("price", self._price), ("oi", self._oi), ("funding", self._funding)):
            try:
                fn(sym, t0, t_ref, ev)
            except RestError as e:
                ev["errors"].append(f"{name}: {e}")
            except Exception as e:   # 資料格式非預期等，不能讓它炸掉主流程
                ev["errors"].append(f"{name}: {type(e).__name__}")
        return ev

    def _price(self, sym: str, t0: float, t_ref: float, ev: dict) -> None:
        rows = self.rest.get("/fapi/v1/klines", {
            "symbol": sym, "interval": "1m",
            "startTime": int((t0 - 60) * 1000), "endTime": int((t_ref + 60) * 1000),
            "limit": self.lookback + 5,
        })
        a = b = None   # (openTime_ms, open)：包含 t0 的那根 / 包含 t_ref 的那根
        for r in rows:
            ot, o = int(r[0]), float(r[1])
            if ot <= t0 * 1000:
                a = (ot, o)
            if ot <= t_ref * 1000:
                b = (ot, o)
        if a and b and b[0] > a[0] and a[1] > 0:
            ev["price_pct"] = (b[1] / a[1] - 1.0) * 100.0
            ev["price_span_min"] = (b[0] - a[0]) / 60000.0

    def _oi(self, sym: str, t0: float, t_ref: float, ev: dict) -> None:
        rows = self.rest.get("/futures/data/openInterestHist", {
            "symbol": sym, "period": "5m",
            "startTime": int((t0 - 600) * 1000), "endTime": int((t_ref + 300) * 1000),
            "limit": self.lookback // 5 + 8,
        })
        a = b = None   # 用幣本位數量 sumOpenInterest，避免價格下跌造成 USD 計價 OI 的假性下降
        for r in rows:
            ts, v = int(r["timestamp"]), float(r["sumOpenInterest"])
            if ts <= t0 * 1000:
                a = (ts, v)
            if ts <= t_ref * 1000:
                b = (ts, v)
        if a and b and b[0] > a[0] and a[1] > 0:
            ev["oi_pct"] = (b[1] / a[1] - 1.0) * 100.0
            ev["oi_span_min"] = (b[0] - a[0]) / 60000.0

    def _funding(self, sym: str, t0: float, t_ref: float, ev: dict) -> None:
        d = self.rest.get("/fapi/v1/premiumIndex", {"symbol": sym})
        if isinstance(d, list):
            d = d[0] if d else {}
        fr = d.get("lastFundingRate")
        if fr not in (None, ""):
            ev["funding_pct"] = float(fr) * 100.0


def make_engine(cfg: dict) -> PrecursorEngine | None:
    p = cfg["precursor"]
    if not p.get("enabled"):
        return None
    rest = BinanceREST(cfg["source"]["rest_url"], float(p["request_timeout"]), float(p["backoff_seconds"]))
    return PrecursorEngine(rest, int(p["lookback_minutes"]), int(p["max_workers"]))


def pc_params(cfg: dict) -> dict:
    p = cfg["precursor"]
    w = [float(p["weight_price"]), float(p["weight_oi"]), float(p["weight_funding"])]
    tot = sum(w) or 1.0
    return {
        "top_k": int(p["top_k"]), "include_first": bool(p["include_first_seen"]),
        "wait_seconds": float(p["wait_seconds"]), "cooldown_seconds": float(p["cooldown_seconds"]),
        "min_score": float(p["min_score"]),
        "price_scale": float(p["price_scale_pct"]), "oi_scale": float(p["oi_scale_pct"]),
        "funding_scale": float(p["funding_scale_pct"]),
        "w_price": w[0] / tot, "w_oi": w[1] / tot, "w_fund": w[2] / tot,
    }


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def score_evidence(e: dict, dominant: str, pc: dict) -> tuple[float, int]:
    """可解釋的啟發式分數（0~1）。缺少的證據視為 0（沒有證據就不加分）。

    dominant = "LONG"：多單被爆為主 -> 前兆是「下跌、OI 堆高、資金費率偏正（多頭擁擠）」
    dominant = "SHORT"：空單被爆為主 -> 前兆是「上漲、OI 堆高、資金費率偏負（空頭擁擠）」
    """
    score, n = 0.0, 0
    if e.get("price_pct") is not None:
        adverse = -e["price_pct"] if dominant == "LONG" else e["price_pct"]
        score += pc["w_price"] * _clamp01(adverse / pc["price_scale"])
        n += 1
    if e.get("oi_pct") is not None:
        score += pc["w_oi"] * _clamp01(e["oi_pct"] / pc["oi_scale"])
        n += 1
    if e.get("funding_pct") is not None:
        crowd = e["funding_pct"] if dominant == "LONG" else -e["funding_pct"]
        score += pc["w_fund"] * _clamp01(crowd / pc["funding_scale"])
        n += 1
    return score, n


def rank_candidates(cands: list[tuple], evid: dict, dominant: str, pc: dict, t_ref: float) -> list[dict]:
    """cands: [(symbol, window_notional, first_seen_ts)] -> 依分數排序的完整資料列（可直接入庫）。"""
    rows = []
    for sym, notional, first_ts in cands:
        e = evid.get(sym) or {}
        sc, n = score_evidence(e, dominant, pc)
        rows.append({
            "symbol": sym, "rank": 0, "is_ignition": 0, "score": sc,
            "notional_usd": notional, "first_seen_ts": first_ts, "ref_ts": t_ref,
            "price_pct": e.get("price_pct"), "price_span_min": e.get("price_span_min"),
            "oi_pct": e.get("oi_pct"), "oi_span_min": e.get("oi_span_min"),
            "funding_pct": e.get("funding_pct"), "n_evidence": n,
            "errors": json.dumps(e.get("errors") or []),
        })
    rows.sort(key=lambda r: (-r["score"], -(r["notional_usd"] or 0)))
    for i, r in enumerate(rows, 1):
        r["rank"] = i
        r["is_ignition"] = 1 if (i == 1 and r["score"] >= pc["min_score"]) else 0
    return rows


def fmt_evidence(r: dict) -> str:
    parts = []
    if r.get("price_pct") is not None:
        parts.append(f"Price {r['price_pct']:+.1f}%/{r['price_span_min']:.0f}m")
    if r.get("oi_pct") is not None:
        parts.append(f"OI {r['oi_pct']:+.1f}%/{r['oi_span_min']:.0f}m")
    if r.get("funding_pct") is not None:
        parts.append(f"FR {r['funding_pct']:+.3f}%")
    return " · ".join(parts) if parts else "no precursor data"


def normalize_ws_url(url: str) -> str:
    """舊版 /ws/ 路徑已於 2026-04-23 停用：自動改寫成 /market/ws/，避免「連上了卻沒資料」。"""
    legacy = "wss://fstream.binance.com/ws/"
    if url.startswith(legacy):
        new = "wss://fstream.binance.com/market/ws/" + url[len(legacy):]
        log.warning("ws_url uses Binance's retired legacy path; auto-switching to %s", new)
        return new
    return url


# --------------------------------------------------------------------------
# Telegram：純 urllib + 背景線程（不阻塞事件迴圈、不需要 aiohttp / requests）
# --------------------------------------------------------------------------
_MD_SPECIAL = re.compile(r"([_*\[\]()~`>#+\-=|{}.!\\])")


def md(s) -> str:
    """跳脫 MarkdownV2 保留字元。所有動態內容都必須先經過這裡。"""
    return _MD_SPECIAL.sub(r"\\\1", str(s))


def _plain(text: str) -> str:
    """MarkdownV2 -> 純文字（解析失敗時的備援）。"""
    return re.sub(r"\\(.)", r"\1", text).replace("*", "").replace("`", "")


class TelegramNotifier:
    API = "https://api.telegram.org"   # 測試時可改成本機假伺服器

    def __init__(self, token: str, chat_id: str, timeout: float = 10.0) -> None:
        self.token = token
        self.chat_id = chat_id
        self.timeout = timeout
        self.q: queue.Queue = queue.Queue(maxsize=100)
        self.thread = threading.Thread(target=self._worker, name="tg-notifier", daemon=True)
        self.thread.start()

    def send(self, text: str) -> None:
        """非阻塞：丟進佇列即返回，由背景線程實際送出。"""
        try:
            self.q.put_nowait(text)
        except queue.Full:
            log.warning("telegram queue full, message dropped")

    def close(self, timeout: float = 8.0) -> None:
        """等待佇列送完（最多 timeout 秒），確保結束訊息不會被吞掉。"""
        try:
            self.q.put(None, timeout=1)
        except queue.Full:
            return
        self.thread.join(timeout)

    def _worker(self) -> None:
        while True:
            item = self.q.get()
            if item is None:
                return
            try:
                self.deliver(item)
            except Exception:
                log.exception("telegram worker error")

    def _post(self, text: str, parse_mode: str | None) -> None:
        payload = {"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        req = urllib.request.Request(
            f"{self.API}/bot{self.token}/sendMessage",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as resp:
            resp.read()

    def deliver(self, text: str) -> bool:
        """同步送出（含重試）。回傳是否成功。日誌中絕不輸出 token / URL。"""
        parse_mode: str | None = "MarkdownV2"
        for attempt in range(3):
            try:
                self._post(text, parse_mode)
                return True
            except urllib.error.HTTPError as e:
                try:
                    body = json.loads(e.read().decode("utf-8", "replace"))
                except Exception:
                    body = {}
                desc = body.get("description", "")
                if e.code == 429:
                    wait = float(body.get("parameters", {}).get("retry_after", 3))
                    log.warning("telegram rate limited, retry in %.0fs", wait)
                    time.sleep(min(wait, 30))
                    continue
                if e.code == 400 and "parse" in desc.lower() and parse_mode:
                    log.warning("telegram rejected MarkdownV2 (%s); resending as plain text", desc)
                    text, parse_mode = _plain(text), None
                    continue
                log.warning("telegram HTTP %s: %s", e.code, desc or e.reason)
                return False
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                reason = getattr(e, "reason", e)
                log.warning("telegram network error (%s: %s), attempt %d/3",
                            type(e).__name__, reason, attempt + 1)
                time.sleep(2 ** attempt)
        return False


def make_notifier(cfg: dict, force: bool = False) -> TelegramNotifier | None:
    t = cfg.get("telegram", {})
    if not (t.get("enabled") or force):
        return None
    # 環境變數優先，避免把 token 寫進檔案後不小心 commit 到 GitHub
    token = os.environ.get("FUSETRACE_TG_TOKEN") or str(t.get("bot_token") or "")
    chat = os.environ.get("FUSETRACE_TG_CHAT_ID") or str(t.get("chat_id") or "")
    if ":" not in token or not chat or chat.startswith("YOUR_"):
        log.warning("telegram enabled but bot_token / chat_id not configured -> notifications off")
        return None
    return TelegramNotifier(token, chat, float(t.get("timeout_seconds", 10)))


def _top_line(items: list[tuple[str, float]]) -> str:
    return " · ".join(f"{s} {fmt_usd(v)}" for s, v in items)


@dataclass(slots=True)
class OpenSnap:
    """觸發當下的視窗快照。背景取數期間事件仍在累積，訊息必須使用觸發時的數字。"""
    total: float
    long: float
    short: float
    n_symbols: int
    top: list
    first: tuple | None
    now: float


def msg_open(eid: str, snap: OpenSnap, window_s: float, attr: list | None) -> str:
    total = snap.total or 1.0
    long_pct = 100.0 * snap.long / total
    short_pct = 100.0 - long_pct
    dom = "📉" if snap.long >= snap.short else "📈"
    first_sym, first_ts = snap.first if snap.first else ("?", snap.now)

    t_window = md(f"{int(window_s)}s window")
    t_total = md(fmt_usd(snap.total))
    t_long = md(f"{long_pct:.0f}%")
    t_short = md(f"{short_pct:.0f}%")
    t_top = md(_top_line(snap.top))
    t_note = md("Statistical inference from public data, not financial advice.")

    if attr and attr[0]["is_ignition"]:
        best = attr[0]
        seen = fmt_ts(best["first_seen_ts"] or first_ts)
        ign = [
            f"🧨 *Likely ignition*: *{md(best['symbol'])}* {md('(score %.2f)' % best['score'])}",
            f"      📊 {md(fmt_evidence(best))}",
            f"      {md('first liquidation seen ' + seen)}",
        ]
    else:
        lead = max(0.0, snap.now - first_ts)
        ign = [
            f"🧨 *First seen*: {md(f'{first_sym} @ {fmt_ts(first_ts)}')}",
            f"      {md(f'(first seen {lead:.0f}s before trigger)')}",
        ]
        if attr:
            ign.append(f"      ⚠️ {md('no clear precursors (best score %.2f)' % attr[0]['score'])}")
    if attr and len(attr) > 1:
        others = " · ".join("%s %.2f" % (r["symbol"], r["score"]) for r in attr[1:])
        ign.append(f"🔎 {md('Also checked: ' + others)}")

    return "\n".join([
        "🚨 *CASCADE DETECTED*",
        f"🆔 `{eid}`",
        "",
        f"💥 *{t_window}*: *{t_total}*",
        f"{dom} Long liq *{t_long}* · Short liq *{t_short}*",
        f"🔥 Symbols firing: *{snap.n_symbols}*",
        *ign,
        f"🏆 Top: {t_top}",
        "",
        f"_{t_note}_",
    ])


def msg_close(ev: Event, status: str, attr: list | None = None) -> str:
    total = ev.total or 1.0
    long_pct = 100.0 * ev.long / total
    short_pct = 100.0 - long_pct
    dur = (ev.last_ts or 0.0) - (ev.started_at or 0.0)
    head = "✅ *CASCADE CLOSED*" if status == "closed" else "⚠️ *CASCADE INTERRUPTED*"
    first_sym, first_ts = ev.first if ev.first else ("?", 0.0)

    if attr and attr[0]["is_ignition"]:
        best = attr[0]
        ign = [
            f"🧨 Ignition: *{md(best['symbol'])}* {md('(score %.2f)' % best['score'])}",
            f"      📊 {md(fmt_evidence(best))}",
        ]
    else:
        ign = [f"🧨 First seen: {md(f'{first_sym} @ {fmt_ts(first_ts)}')}"]

    return "\n".join([
        head,
        f"🆔 `{ev.id}`",
        "",
        f"⏱ Duration: *{md(f'{dur:.0f}s')}*",
        f"💰 Total: *{md(fmt_usd(ev.total))}* {md(f'({ev.n} liquidations)')}",
        f"📈 Peak {md('window')}: *{md(fmt_usd(ev.peak))}*",
        f"⚖️ Long liq *{md(f'{long_pct:.0f}%')}* · Short liq *{md(f'{short_pct:.0f}%')}*",
        f"🔥 Symbols: *{len(ev.by_symbol)}*",
        *ign,
        f"🏆 Top: {md(_top_line(ev.top(3)))}",
        "",
        f"🔁 `python fusetrace.py --replay {ev.id}`",
    ])


# --------------------------------------------------------------------------
# Cascade 偵測器
# --------------------------------------------------------------------------
class CascadeDetector:
    def __init__(self, cfg: dict, db: Database, notifier: TelegramNotifier | None = None,
                 engine: PrecursorEngine | None = None) -> None:
        d = cfg["detection"]
        self.db = db
        self.notifier = notifier
        self.engine = engine
        self.pc = pc_params(cfg)
        self.tg_open = bool(cfg["telegram"].get("notify_on_open", True))
        self.tg_close = bool(cfg["telegram"].get("notify_on_close", True))
        self.window_s = float(d["window_seconds"])
        self.min_total = float(d["min_total_usd"])
        self.min_symbols = int(d["min_symbols"])
        self.mult = float(d["baseline_multiplier"])
        self.min_base_min = int(d["min_baseline_minutes"])
        self.end_usd = self.min_total * float(d["end_ratio"])
        self.quiet = float(d["quiet_seconds"])
        self.max_tl = int(d["max_timeline_rows"])
        self.store_all = bool(cfg["storage"]["store_all_liquidations"])

        self.window = RollingWindow(self.window_s)
        self.baseline = Baseline(int(d["baseline_minutes"]))
        self.event: Event | None = None
        self.pending_liqs: list[tuple] = []
        self.msg_count = 0
        self.started = time.time()
        self.last_msg = 0.0
        self._last_save = 0.0
        self._last_enrich = -1e9
        self._tasks: set[asyncio.Task] = set()
        self._attr: dict[str, list] = {}   # event_id -> 前兆排名（供 CLOSE 訊息使用）

    # ----------------------------------------------------------- 資料進入
    def on_liq(self, liq: Liq) -> None:
        self.msg_count += 1
        self.last_msg = liq.arrival
        self.window.add(liq)
        self.baseline.add(liq.arrival, liq.notional)
        if self.store_all:
            self.pending_liqs.append(liq.row())
        if self.event:
            self.event.add(liq)

    # ----------------------------------------------------------- 週期檢查
    def tick(self, now: float) -> None:
        self.window.expire(now)
        w = self.window
        ev = self.event
        if ev is None:
            if self._should_trigger(now):
                self._open(now)
        else:
            ev.peak = max(ev.peak, w.total)
            if w.total >= self.end_usd:
                ev.last_hot = now
            elif now - ev.last_hot >= self.quiet:
                self._close("closed")
        self.flush(now)

    def _should_trigger(self, now: float) -> bool:
        w = self.window
        if w.total < self.min_total or w.n_symbols < self.min_symbols:
            return False
        base = self.baseline.avg_per_minute(now, self.window_s, self.min_base_min)
        if base is None:      # 基線暖機中：只用絕對閾值
            return True
        return w.total >= self.mult * base * (self.window_s / 60.0)

    def _notify(self, enabled: bool, build) -> None:
        """組訊息並丟給背景線程；任何錯誤都不能影響偵測主流程。"""
        if not (self.notifier and enabled):
            return
        try:
            self.notifier.send(build())
        except Exception:
            log.exception("failed to build/send telegram message")

    # ----------------------------------------------------------- 事件開始
    def _candidates(self, ev: Event) -> list[tuple]:
        """候選起火點：視窗內爆倉額前 K 名，加上最早出現爆倉的幣種。"""
        w = self.window
        top = sorted(w.by_symbol.items(), key=lambda kv: kv[1], reverse=True)[: self.pc["top_k"]]
        first_by: dict[str, float] = {}
        for l in w.items:
            first_by.setdefault(l.symbol, l.ts)
        syms = [s for s, _ in top]
        if self.pc["include_first"] and ev.first and ev.first[0] not in syms:
            syms.append(ev.first[0])
        return [(s, w.by_symbol.get(s, 0.0), first_by.get(s)) for s in syms]

    def _open(self, now: float) -> None:
        w = self.window
        eid = time.strftime("%Y%m%d-%H%M%S", time.gmtime(now))
        ev = Event(eid, w.total, now, self.max_tl)
        for l in list(w.items):          # 回填視窗內已發生的爆倉，時間軸才完整
            ev.add(l)
        self.event = ev
        self.db.upsert_event(ev.to_row())
        long_pct = 100 * w.long / w.total if w.total else 0
        log.warning(
            "🚨 CASCADE OPEN  id=%s  window=%s  long=%.0f%%  symbols=%d  first=%s@%s  top: %s",
            eid, fmt_usd(w.total), long_pct, w.n_symbols,
            ev.first[0] if ev.first else "?", fmt_ts(ev.first[1]) if ev.first else "?",
            _top_line(ev.top(3)),
        )
        snap = OpenSnap(w.total, w.long, w.short, w.n_symbols, ev.top(3), ev.first, now)
        cands = self._candidates(ev)
        use_engine = (self.engine is not None and cands
                      and now - self._last_enrich >= self.pc["cooldown_seconds"])
        if use_engine:
            self._last_enrich = now
            dominant = "LONG" if w.long >= w.short else "SHORT"
            t_ref = ev.first[1] if ev.first else now
            task = asyncio.get_running_loop().create_task(
                self._enrich(eid, snap, cands, dominant, t_ref))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        else:
            self._notify(self.tg_open, lambda: msg_open(eid, snap, self.window_s, None))

    async def _enrich(self, eid: str, snap: OpenSnap, cands: list[tuple], dominant: str,
                      t_ref: float) -> None:
        """背景取數 -> 排名 -> 發 OPEN 警報（最多等 wait_seconds）-> 入庫。不阻塞偵測迴圈。"""
        loop = asyncio.get_running_loop()
        symbols = [c[0] for c in cands]
        fut = loop.run_in_executor(None, self.engine.collect, symbols, t_ref)
        sent = False
        attr: list | None = None
        try:
            evid = None
            try:
                evid = await asyncio.wait_for(asyncio.shield(fut), self.pc["wait_seconds"])
            except asyncio.TimeoutError:
                log.info("precursor lookup slow; sending alert without it, will finish in background")
            except Exception as e:
                log.warning("precursor lookup failed: %s: %s", type(e).__name__, e)
            if evid:
                attr = rank_candidates(cands, evid, dominant, self.pc, t_ref)
                self._attr[eid] = attr
            self._notify(self.tg_open, lambda: msg_open(eid, snap, self.window_s, attr))
            sent = True
            if evid is None and not fut.done():     # 逾時：背景繼續等，供入庫與 CLOSE 訊息使用
                try:
                    evid = await fut
                except Exception:
                    evid = None
                if evid:
                    attr = rank_candidates(cands, evid, dominant, self.pc, t_ref)
                    self._attr[eid] = attr
            elif evid is None and fut.done():
                pass
            if attr:
                self.db.save_precursors(eid, attr)
                best = attr[0]
                log.warning("🎯 PRECURSORS id=%s  %s  score=%.2f  %s  checked=%s",
                            eid, "IGNITION=" + best["symbol"] if best["is_ignition"] else "no clear ignition",
                            best["score"], fmt_evidence(best),
                            ",".join(f"{r['symbol']}:{r['score']:.2f}" for r in attr))
        except asyncio.CancelledError:
            if not sent:   # 程式關閉時仍要把 OPEN 警報送出（無前兆資料）
                self._notify(self.tg_open, lambda: msg_open(eid, snap, self.window_s, None))
            raise
        except Exception:
            log.exception("precursor enrichment failed")
            if not sent:
                self._notify(self.tg_open, lambda: msg_open(eid, snap, self.window_s, None))

    # ----------------------------------------------------------- 事件結束
    def _close(self, status: str) -> None:
        ev = self.event
        if ev is None:
            return
        ev.status = status
        self.event = None
        attr = self._attr.pop(ev.id, None)
        try:
            if ev.pending:
                self.db.add_timeline(ev.pending)
                ev.pending.clear()
            self.db.upsert_event(ev.to_row())
        except Exception:
            log.exception("failed to finalize event %s", ev.id)
        dur = (ev.last_ts or 0) - (ev.started_at or 0)
        long_pct = 100 * ev.long / ev.total if ev.total else 0
        log.warning(
            "✅ CASCADE %s id=%s  duration=%.0fs  total=%s  peak60=%s  long=%.0f%%  n=%d  top: %s",
            status.upper(), ev.id, dur, fmt_usd(ev.total), fmt_usd(ev.peak), long_pct, ev.n,
            _top_line(ev.top(3)),
        )
        self._notify(self.tg_close, lambda: msg_close(ev, status, attr))

    # ----------------------------------------------------------- 寫入資料庫
    def flush(self, now: float | None = None) -> None:
        try:
            if self.pending_liqs:
                self.db.insert_liquidations(self.pending_liqs)
                self.pending_liqs.clear()
            ev = self.event
            if ev:
                if ev.pending:
                    self.db.add_timeline(ev.pending)
                    ev.pending.clear()
                if now is not None and now - self._last_save >= 10:   # 每 10 秒更新一次事件摘要
                    self.db.upsert_event(ev.to_row())
                    self._last_save = now
        except Exception:
            log.exception("database flush failed (will retry next tick)")

    async def drain(self) -> None:
        """關閉時：取消進行中的前兆取數（其 CancelledError 處理會補送 OPEN 警報）。"""
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def shutdown(self) -> None:
        self.flush()
        if self.event:
            self._close("interrupted")


# --------------------------------------------------------------------------
# WebSocket 監聽（斷線自動重連，指數退避）
# --------------------------------------------------------------------------
def handle_raw(raw: str | bytes, det: CascadeDetector) -> None:
    msg = json.loads(raw)
    items = msg if isinstance(msg, list) else [msg]
    for m in items:
        if isinstance(m, dict) and "data" in m:   # 相容 combined stream 包裝
            m = m["data"]
        liq = parse_force_order(m)
        if liq:
            det.on_liq(liq)


async def ws_loop(url: str, det: CascadeDetector) -> None:
    try:
        import websockets
    except ImportError:
        log.error("missing dependency: pip install websockets")
        raise SystemExit(1)

    backoff = 1.0
    while True:
        try:
            log.info("connecting %s", url)
            async with websockets.connect(url, ping_interval=20, ping_timeout=20, max_queue=1024) as ws:
                log.info("connected, listening for liquidations...")
                backoff = 1.0
                async for raw in ws:
                    try:
                        handle_raw(raw, det)
                    except Exception:
                        log.exception("bad message skipped")
            log.warning("connection closed by server (Binance rotates connections every ~24h)")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # 網路錯誤、握手失敗、逾時…
            log.warning("ws error: %s: %s", type(e).__name__, e)
        log.info("reconnecting in %.0fs", backoff)
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60.0)


# --------------------------------------------------------------------------
# 模擬器：用合成資料走一遍完整流程（ETH 先爆 -> 擴散到其他幣種 -> 平息）
# --------------------------------------------------------------------------
async def simulate_feed(det: CascadeDetector, stop: asyncio.Event) -> None:
    prices = {"ETHUSDT": 3000, "SOLUSDT": 150, "BTCUSDT": 65000, "DOGEUSDT": 0.15,
              "XRPUSDT": 0.6, "ARBUSDT": 0.9, "AVAXUSDT": 35, "LINKUSDT": 15}
    syms = list(prices)
    t0 = time.time()
    log.info("[simulate] 8s calm -> 17s cascade -> calm. Watch for CASCADE OPEN/CLOSED logs.")
    try:
        while time.time() - t0 < 70:
            el = time.time() - t0
            burst = 8 <= el < 25
            n = random.randint(8, 15) if burst else random.randint(0, 1)
            for _ in range(n):
                if burst and el < 12 and random.random() < 0.7:
                    sym = "ETHUSDT"                       # 起火點先出現在 ETH
                else:
                    sym = random.choice(syms if burst else syms[:3])
                usd = random.uniform(20_000, 150_000) if burst else random.uniform(2_000, 30_000)
                side = "LONG" if random.random() < (0.9 if burst else 0.5) else "SHORT"
                now = time.time()
                det.on_liq(Liq(now, now, sym, side, prices[sym], usd / prices[sym], usd))
            await asyncio.sleep(1)
    finally:
        stop.set()


# --------------------------------------------------------------------------
# 主流程
# --------------------------------------------------------------------------
async def ticker(det: CascadeDetector, db: Database, retention_days: float) -> None:
    last_purge = 0.0
    last_beat = time.time()
    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        det.tick(now)
        if now - last_beat >= 60:
            last_beat = now
            log.info("heartbeat: msgs=%d  window=%s (%d symbols)  event=%s",
                     det.msg_count, fmt_usd(det.window.total), det.window.n_symbols,
                     det.event.id if det.event else "-")
            idle = now - (det.last_msg or det.started)
            if idle > 300:   # 清算在 Binance 上幾乎隨時都在發生，5 分鐘完全沒資料通常代表訂閱路徑有問題
                log.warning("no liquidation messages for %.0fs. If this persists, check source.ws_url "
                            "(Binance moved !forceOrder@arr to the /market path in 2026).", idle)
        if now - last_purge >= 3600:
            last_purge = now
            try:
                db.purge(retention_days)
            except Exception:
                log.exception("purge failed")


async def run(cfg: dict, simulate: bool) -> None:
    db = Database(cfg["storage"]["path"])
    stale = db.recover_open_events()
    if stale:
        log.info("marked %d unfinished event(s) from previous run as 'interrupted'", stale)
    notifier = make_notifier(cfg)
    engine = make_engine(cfg)
    det = CascadeDetector(cfg, db, notifier, engine)
    stop = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):   # Windows 不支援，改走 KeyboardInterrupt
            pass

    d = cfg["detection"]
    log.info("FuseTrace started | window=%ss min_total=%s min_symbols=%s baseline x%s | db=%s | "
             "telegram=%s | precursor=%s",
             d["window_seconds"], fmt_usd(float(d["min_total_usd"])), d["min_symbols"],
             d["baseline_multiplier"], db.path, "on" if notifier else "off",
             f"on ({cfg['precursor']['lookback_minutes']}m lookback)" if engine else "off")
    if simulate and engine:
        log.info("[simulate] cascade is synthetic, but precursor evidence is REAL Binance REST data "
                 "(set precursor.enabled: false to skip network calls)")

    tasks = [asyncio.create_task(ticker(det, db, float(cfg["storage"]["retention_days"])))]
    if simulate:
        tasks.append(asyncio.create_task(simulate_feed(det, stop)))
    else:
        url = normalize_ws_url(cfg["source"]["ws_url"])
        tasks.append(asyncio.create_task(ws_loop(url, det)))
    try:
        await stop.wait()
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await det.drain()
        det.shutdown()
        if notifier:
            notifier.close()   # 等最後一則訊息送完
        db.close()
        log.info("shutdown complete")


# --------------------------------------------------------------------------
# CLI：事件列表 / Replay / Telegram 測試
# --------------------------------------------------------------------------
def list_events(cfg: dict) -> None:
    if not Path(cfg["storage"]["path"]).exists():
        print(f"database not found: {cfg['storage']['path']}")
        return
    db = Database(cfg["storage"]["path"])
    rows = db.list_events(20)
    if not rows:
        print("no events recorded yet")
    for r in rows:
        dur = (r["ended_at"] or r["started_at"]) - r["started_at"]
        long_pct = 100 * r["long_usd"] / r["total_usd"] if r["total_usd"] else 0
        print(f"{r['id']}  {r['status']:<11} total={fmt_usd(r['total_usd'] or 0):>9}  "
              f"peak={fmt_usd(r['peak_window_usd'] or 0):>9}  long={long_pct:3.0f}%  "
              f"dur={dur:4.0f}s  first={r['first_symbol']}")
    db.close()


def _ts_full(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(ts)) + f".{int(ts % 1 * 1000):03d} UTC"


def _ts_ms(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.gmtime(ts)) + f".{int(ts % 1 * 1000):03d}"


def _find_events(db: Database, ref: str) -> list:
    c = db.conn
    if ref == "latest":
        return c.execute("SELECT * FROM events ORDER BY started_at DESC LIMIT 1").fetchall()
    rows = c.execute("SELECT * FROM events WHERE id=?", (ref,)).fetchall()
    if not rows:   # 允許只輸入前綴，例如 20261101-1252
        rows = c.execute("SELECT * FROM events WHERE id LIKE ? ORDER BY started_at DESC",
                         (ref + "%",)).fetchall()
    return rows


def replay(cfg: dict, ref: str, max_rows: int) -> int:
    """從資料庫重建事件：摘要 -> 擴散順序 -> 強度分佈 -> 逐筆時間軸 -> 一致性檢查。純離線。"""
    path = cfg["storage"]["path"]
    if not Path(path).exists():
        print(f"database not found: {path}  (use --db to point at another file)")
        return 1
    db = Database(path)
    try:
        rows = _find_events(db, ref)
        if not rows:
            print(f"event '{ref}' not found. Try: python fusetrace.py --list-events")
            return 1
        if len(rows) > 1:
            print(f"'{ref}' matches {len(rows)} events, be more specific:")
            for r in rows:
                print(f"  {r['id']}")
            return 1
        ev = rows[0]
        tl = db.get_timeline(ev["id"])
        _print_replay(db, ev, tl, max_rows)
        return 0
    finally:
        db.close()


def _print_replay(db: Database, ev, tl: list, max_rows: int) -> None:
    W = 70
    total = ev["total_usd"] or 0.0
    long_pct = 100 * (ev["long_usd"] or 0) / total if total else 0.0
    print("═" * W)
    print(" 🧨 FuseTrace REPLAY")
    print("═" * W)

    if not tl:
        print(f" Event : {ev['id']}  [{ev['status']}]")
        print(" (no timeline rows stored for this event)")
        return

    t0, t1 = tl[0]["ts"], tl[-1]["ts"]
    dur = t1 - t0
    print(f" Event        : {ev['id']}  [{ev['status']}]")
    print(f" Started      : {_ts_full(t0)}")
    print(f" Duration     : {dur:.1f}s")
    print(f" Total        : {fmt_usd(total)}  ({ev['n_liquidations']} liquidations, "
          f"{ev['n_symbols']} symbols)")
    print(f" Trigger/Peak : {fmt_usd(ev['trigger_total_usd'] or 0)} at trigger / "
          f"{fmt_usd(ev['peak_window_usd'] or 0)} peak window")
    print(f" Long / Short : {long_pct:.0f}% / {100 - long_pct:.0f}%  (share of liquidated notional)")
    pre = [dict(r) for r in db.get_precursors(ev["id"])]
    best = pre[0] if pre else None
    if best and best["is_ignition"]:
        print(f" 🧨 Likely ignition (precursor-ranked): {best['symbol']}   score {best['score']:.2f}")
        print(f"      evidence: {fmt_evidence(best)}")
        if best["symbol"] != ev["first_symbol"]:
            print(f"      ℹ first liquidation was on {ev['first_symbol']} @ "
                  f"{_ts_ms(ev['first_ts'] or t0)} UTC, but {best['symbol']} shows stronger pre-event evidence")
    else:
        print(f" 🧨 First seen: {ev['first_symbol']} @ {_ts_ms(ev['first_ts'] or t0)} UTC")
        if best:
            print(f"      ⚠️  no clear precursors (best score {best['score']:.2f}, below threshold): "
                  f"treat first-seen as weak evidence")
    if ev["status"] != "closed":
        print(f" ⚠️  status is '{ev['status']}': the event did not end cleanly, totals may be partial")

    # ---- 前兆證據（事件發生當下取得並存入 SQLite；replay 本身完全離線）
    if pre:
        ref = pre[0]["ref_ts"] or t0
        title = f"Precursor evidence (before first liquidation @ {_ts_ms(ref)} UTC)"
        print(f"\n── {title} " + "─" * max(3, W - len(title) - 4))
        print(f"   {'symbol':<12}{'liq $':>9}  {'price':>13}  {'OI':>13}  {'funding':>9}  {'score':>5}")
        for r in pre:
            star = "★" if r["is_ignition"] else " "
            price = f"{r['price_pct']:+.1f}%/{r['price_span_min']:.0f}m" if r["price_pct"] is not None else "—"
            oi = f"{r['oi_pct']:+.1f}%/{r['oi_span_min']:.0f}m" if r["oi_pct"] is not None else "—"
            fr = f"{r['funding_pct']:+.3f}%" if r["funding_pct"] is not None else "—"
            print(f"  {star} {r['symbol']:<12}{fmt_usd(r['notional_usd'] or 0):>9}  {price:>13}  "
                  f"{oi:>13}  {fr:>9}  {r['score']:>5.2f}")
            errs = json.loads(r["errors"] or "[]")
            if errs:
                print(f"      ! {'; '.join(errs)}")
        print("  ★ = selected as likely ignition. score = weighted heuristic of price / OI / funding")
        print("  (weights and scales are in config.yaml), not a probability. OI uses coin-denominated size.")
    else:
        print("\n── Precursor evidence " + "─" * (W - 22))
        print("  (none recorded: event predates this feature, or lookup was disabled / unavailable)")

    # ---- 擴散順序：每個幣種第一次出現的時間
    seen: dict[str, list] = {}
    for r in tl:
        s = seen.setdefault(r["symbol"], [r["ts"], r["notional_usd"], 0.0])
        s[2] += r["notional_usd"]
    print("\n── Spread order (when each symbol first lit up) " + "─" * (W - 49))
    for i, (sym, (fts, first_usd, tot)) in enumerate(seen.items(), 1):
        if i > 12:
            print(f"   ... +{len(seen) - 12} more symbols")
            break
        print(f"  #{i:<2} T+{fts - t0:7.2f}s  {sym:<12} first {fmt_usd(first_usd):>8}   "
              f"total {fmt_usd(tot):>9}")

    # ---- 強度分佈
    bsec = max(1, math.ceil(dur / 20)) if dur > 0 else 1
    buckets: dict[int, list] = {}
    for r in tl:
        b = buckets.setdefault(int((r["ts"] - t0) // bsec), [0.0, 0])
        b[0] += r["notional_usd"]
        b[1] += 1
    mx = max(v[0] for v in buckets.values()) or 1.0
    print(f"\n── Intensity ({bsec}s buckets) " + "─" * (W - 28 - len(str(bsec))))
    for i in range(max(buckets) + 1):
        usd, n = buckets.get(i, (0.0, 0))
        bar = "█" * int(round(30 * usd / mx))
        print(f"  T+{i * bsec:>4}s  {bar:<30} {fmt_usd(usd):>9}  ({n})")

    # ---- 逐筆時間軸
    print("\n── Timeline " + "─" * (W - 12))
    print("  🔴 = long position liquidated   🟢 = short position liquidated")
    cum = 0.0
    shown = 0
    for r in tl:
        cum += r["notional_usd"]
        if max_rows and shown >= max_rows:
            continue
        icon = "🔴" if r["side"] == "LONG" else "🟢"
        print(f"  T+{r['ts'] - t0:7.2f}s {_ts_ms(r['ts'])} {icon} {r['symbol']:<12} "
              f"{fmt_usd(r['notional_usd']):>9} @ {r['price']:<10g} Σ {fmt_usd(cum):>9}")
        shown += 1
    if shown < len(tl):
        print(f"  ... {len(tl) - shown} more rows hidden (use --rows 0 to show all)")
    if (ev["n_liquidations"] or 0) > len(tl):
        print(f"  note: timeline capped at {len(tl)} rows (max_timeline_rows); summary totals are complete")

    # ---- 一致性檢查：事件時間軸 vs 原始爆倉表
    tl_sum = sum(r["notional_usd"] for r in tl)
    cnt, raw_sum = db.conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(notional_usd), 0) FROM liquidations WHERE ts BETWEEN ? AND ?",
        (t0, t1),
    ).fetchone()
    print("\n── Cross-check vs raw liquidations table " + "─" * (W - 42))
    if cnt == 0:
        print("  raw table has no rows in this range (store_all_liquidations off, or purged)")
    elif cnt == len(tl) and abs(raw_sum - tl_sum) < 1.0:
        print(f"  ✔ consistent: {cnt} rows, {fmt_usd(raw_sum)} in both tables")
    else:
        print(f"  ℹ timeline: {len(tl)} rows / {fmt_usd(tl_sum)}   raw: {cnt} rows / {fmt_usd(raw_sum)}")
        print("    small differences can come from timeline caps or exchange-timestamp ordering")
    print("═" * W)
    print(" Replay read only the local SQLite file. No network, no live data.")


def probe(cfg: dict, symbol: str) -> int:
    """冒煙測試：對單一幣種跑一次前兆取數，驗證三個公開 REST 端點在你的網路環境可用。"""
    cfg = {**cfg, "precursor": {**cfg["precursor"], "enabled": True}}
    engine = make_engine(cfg)
    sym = symbol.upper()
    print(f"probing {sym} | base={cfg['source']['rest_url']} | lookback={cfg['precursor']['lookback_minutes']}m "
          f"| reference time = now-120s")
    ev = engine.collect([sym], time.time() - 120)[sym]
    print(json.dumps(ev, indent=2))
    print(f"requests sent: {engine.rest.n_requests} | X-MBX-USED-WEIGHT-1M: {engine.rest.used_weight}")
    ok = not ev["errors"]
    print("✅ klines / openInterestHist / premiumIndex all OK" if ok
          else "❌ some endpoints failed (see errors above)")
    return 0 if ok else 1


def test_telegram(cfg: dict) -> int:
    n = make_notifier(cfg, force=True)
    if n is None:
        print("Telegram not configured. Set [telegram] bot_token/chat_id in config.yaml, "
              "or env FUSETRACE_TG_TOKEN / FUSETRACE_TG_CHAT_ID.")
        return 1
    ok = n.deliver("\n".join([
        "🧪 *FuseTrace test message*",
        md("If you can read this, Telegram alerts are working. ✅"),
    ]))
    n.close(1)
    print("✅ test message sent" if ok else "❌ failed to send (see log above)")
    return 0 if ok else 1


def main() -> None:
    ap = argparse.ArgumentParser(description="FuseTrace - liquidation cascade detector")
    ap.add_argument("-c", "--config", default="config.yaml")
    ap.add_argument("--db", help="override storage.path (e.g. fusetrace_sim.db)")
    ap.add_argument("--simulate", action="store_true", help="run with synthetic data (no network)")
    ap.add_argument("--list-events", action="store_true", help="print recorded events and exit")
    ap.add_argument("--replay", metavar="EVENT_ID", help="replay a stored event ('latest' or id prefix ok)")
    ap.add_argument("--rows", type=int, default=60, help="max timeline rows in replay (0 = all)")
    ap.add_argument("--test-telegram", action="store_true", help="send a test message and exit")
    ap.add_argument("--probe", metavar="SYMBOL", help="smoke-test precursor REST endpoints, e.g. ETHUSDT")
    args = ap.parse_args()

    cfg = load_config(args.config)
    logging.basicConfig(
        level=getattr(logging, str(cfg["logging"]["level"]).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.db:
        cfg["storage"]["path"] = args.db

    if args.probe:
        sys.exit(probe(cfg, args.probe))
    if args.test_telegram:
        sys.exit(test_telegram(cfg))
    if args.list_events:
        list_events(cfg)
        return
    if args.replay:
        sys.exit(replay(cfg, args.replay, args.rows))

    if args.simulate:   # 模擬模式使用獨立 DB 與較敏感的參數，避免污染正式資料
        if not args.db:
            cfg["storage"]["path"] = "fusetrace_sim.db"
        cfg["detection"].update(window_seconds=20, min_total_usd=2_000_000, min_symbols=3,
                                quiet_seconds=8, min_baseline_minutes=10**6)

    try:
        asyncio.run(run(cfg, args.simulate))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
