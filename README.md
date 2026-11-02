<div align="center">

# 🧨 FuseTrace

### When the market crashes, everyone asks *how much* was liquidated.
### FuseTrace asks: **where did the fuse get lit?**

Real-time liquidation-cascade detector with **precursor attribution** and **offline replay**.
Runs on an old laptop. No GPU. No API keys. No paid data.

[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/)
[![Dependencies](https://img.shields.io/badge/dependencies-1%20(websockets)-brightgreen.svg)](#-installation)
[![No GPU](https://img.shields.io/badge/GPU-not%20required-brightgreen.svg)](#-runs-on-modest-hardware)
[![No API key](https://img.shields.io/badge/exchange%20API%20key-not%20required-brightgreen.svg)](#-data-sources)
[![Telegram alerts](https://img.shields.io/badge/alerts-Telegram-26A5E4.svg)](#-telegram-alerts)
[![Local first](https://img.shields.io/badge/local--first-SQLite-black.svg)](#-how-it-works)

[Quick Start](#-quick-start) · [Features](#-features) · [Usage](#-usage) · [How it works](#-how-it-works) · [Limitations](#-honest-limitations) · [Contributing](#-contributing)

</div>

---

## 😤 The problem

Every flash crash looks the same:

- Headlines say *"$900M liquidated in 24h"*, but nobody tells you **which market moved first**.
- Complete liquidation analytics sit behind paid subscriptions.
- Per-order liquidation bots spam your chat; the signal drowns in noise.

**FuseTrace is not a liquidation ticker. It is an incident investigator.**

It watches Binance's public liquidation stream, detects **liquidation cascades** with a transparent rolling-window rule, then looks *backwards in time* at price, open interest and funding for the symbols involved, and tells you which ones showed pre-event stress. Every event is stored in a local SQLite file so anyone can **replay it offline** and check the conclusion.

> Attribution here means *statistical evidence*, not proof. See [Honest limitations](#-honest-limitations).

---

## ✨ Features

| | |
|---|---|
| 🔥 **Cascade detection** | A 60-second rolling window (pure `collections.deque`) tracks liquidation notional per symbol. A cascade fires only when total size, number of symbols **and** a ratio against the recent baseline all exceed your thresholds. A single-coin wick doesn't count. |
| 🎯 **Precursor attribution** | Upgrades "who was liquidated first" to **"who showed stress before the liquidations started"**. For the top liquidated symbols (plus the first one seen) it checks the **price move, open-interest build-up and funding rate** in the 15 minutes *before* the first liquidation, then ranks candidates with an explainable score. |
| ⏪ **Historical replay** | `--replay` rebuilds any stored event in your terminal, fully offline: summary, precursor table, per-symbol spread order, intensity histogram, tick-by-tick timeline and a cross-check against the raw liquidation table. |
| 🔬 **Retroactive attribution** | `--attribute` re-runs precursor attribution on events you already recorded (history goes back ~30 days for open interest). Tune the weights, re-run, compare. |
| 📲 **Telegram alerts** | Polished MarkdownV2 messages on cascade open and close. Sent from a background thread using only `urllib`; it never blocks the detector. |
| 🧾 **Transparent rules** | Every threshold, scale and weight lives in `config.yaml`. No black box, no LLM, no hidden model. |
| 🔐 **Read-only by design** | Public market data only. No exchange API key, no private key, no trading functionality. |
| 🧪 **Built-in simulator** | `--simulate` walks through a synthetic cascade end to end, so you can test alerts and replay without waiting for a real crash. |

### What an alert looks like

> Sample from `--simulate` (synthetic cascade).

```text
🚨 CASCADE DETECTED
🆔 20261102-004311

💥 20s window: $2.16M
📉 Long liq 94% · Short liq 6%
🔥 Symbols firing: 5
🧨 Likely ignition: ETHUSDT (score 0.96)
      📊 Price -3.0%/15m · OI +4.4%/15m · FR +0.080%
      first liquidation seen 00:43:08 UTC
🔎 Also checked: SOLUSDT 0.07 · AVAXUSDT 0.07 · XRPUSDT 0.07
🏆 Top: ETHUSDT $1.54M · AVAXUSDT $247.5K · XRPUSDT $235.2K

Statistical inference from public data, not financial advice.
```

### What a replay looks like

```text
══════════════════════════════════════════════════════════════════════
 🧨 FuseTrace REPLAY
══════════════════════════════════════════════════════════════════════
 Event        : 20261102-004311  [closed]
 Duration     : 47.1s
 Total        : $16.54M  (212 liquidations, 8 symbols)
 Long / Short : 92% / 8%  (share of liquidated notional)
 🧨 Likely ignition (precursor-ranked): ETHUSDT   score 0.96
      evidence: Price -3.0%/15m · OI +4.4%/15m · FR +0.080%
      ℹ first liquidation was on SOLUSDT, but ETHUSDT shows stronger pre-event evidence

── Precursor evidence (before first liquidation) ──────────────────────
   symbol          liq $          price             OI    funding  score
  ★ ETHUSDT        $1.54M      -3.0%/15m      +4.4%/15m    +0.080%   0.96
    SOLUSDT        $17.3K      -0.2%/15m      +0.2%/15m    +0.004%   0.07

── Spread order (when each symbol first lit up) ───────────────────────
  #1  T+   0.00s  SOLUSDT      first   $17.3K   total    $1.58M
  #2  T+   2.00s  BTCUSDT      first    $6.9K   total    $1.23M
  #3  T+   6.01s  ETHUSDT      first   $22.3K   total    $4.15M
  ...
── Intensity (3s buckets) ─────────────────────────────────────────────
  T+   6s  █████████████████████             $2.11M  (27)
  T+  12s  ██████████████████████████████    $3.09M  (32)
```

The interesting line is the `ℹ`: **the first liquidation and the first sign of stress are often on different symbols.** That gap is exactly what FuseTrace is built to surface.

---

## 🪶 Runs on modest hardware

FuseTrace is built for the "regular geek": a laptop you already own.

- **Pure CPU, event-driven.** Mostly idle; real work happens only when liquidations arrive.
- **No GPU, no model inference.** Detection is a rolling window plus arithmetic; reports are templates.
- **One third-party dependency:** [`websockets`](https://pypi.org/project/websockets/). Everything else is the Python standard library (`asyncio`, `sqlite3`, `urllib`, `collections`).
- **Bounded memory.** A rolling `deque` for the window and at most 60 one-minute baseline buckets.
- **Tiny network footprint.** One WebSocket connection. REST is used **only when a cascade fires**: roughly 12 small public requests per event (about 4 symbols × 3 endpoints), with a cooldown, a concurrency cap of 3 and automatic back-off on HTTP 429/418.
- **Local storage.** A single SQLite file in WAL mode, with automatic retention cleanup (30 days by default).

> We haven't published formal benchmarks yet. If you run FuseTrace on an old laptop, a Raspberry Pi or a small VPS, please share your CPU / RAM numbers in an issue or PR.

---

## 🚀 Quick start

**Requirements:** Python 3.10 or newer.

```bash
# 1. Get the code
git clone https://github.com/<your-username>/fusetrace.git
cd fusetrace

# 2. Create and activate a virtual environment
python3 -m venv .venv
source .venv/bin/activate          # Windows (PowerShell): .venv\Scripts\Activate.ps1
                                   # Windows (cmd):        .venv\Scripts\activate.bat

# 3. Install the only dependency
pip install websockets

# 4. Create your private config from the template
cp config.example.yaml config.yaml # Windows (cmd): copy config.example.yaml config.yaml

# 5. Verify your connection to Binance's public REST endpoints
python fusetrace.py --probe BTCUSDT

# 6. See a full synthetic cascade end to end (no real crash needed)
python fusetrace.py --simulate

# 7. Go live
python fusetrace.py
```

> `PyYAML` is optional. FuseTrace ships with a minimal built-in parser for the two-level `config.yaml`, and uses PyYAML automatically if it happens to be installed.

---

## 🔐 Security first: protect your Telegram token

Your Telegram bot token lets anyone send messages as your bot. **Never commit it to Git.**

> ### ⚠️ Do this before your first commit
>
> 1. **Ignore your private config and local databases.** Add this to `.gitignore`:
>
>    ```gitignore
>    # FuseTrace: never commit secrets or local data
>    config.yaml
>    *.db
>    *.db-wal
>    *.db-shm
>    .venv/
>    __pycache__/
>    ```
>
> 2. **Commit only `config.example.yaml`.** It contains placeholders, never real credentials. Everyone (including you) copies it to `config.yaml` and fills in their own values:
>
>    ```bash
>    cp config.example.yaml config.yaml
>    ```
>
> 3. **Prefer environment variables for secrets.** They take priority over the config file and keep tokens out of your project folder entirely:
>
>    ```bash
>    export FUSETRACE_TG_TOKEN="123456789:AAxxxxxxxxxxxxxxxx"
>    export FUSETRACE_TG_CHAT_ID="-1001234567890"
>    ```
>
> 4. **Already pushed a token by accident?** Treat it as compromised: revoke it with `/revoke` in [@BotFather](https://t.me/BotFather), generate a new one, and remove the old one from your Git history, not just from the latest commit.

Before every push, run `git status` and confirm `config.yaml` is not listed.

---

## 📲 Telegram alerts

1. Open [@BotFather](https://t.me/BotFather), run `/newbot`, and copy the token.
2. Send any message to your bot (or add it to a group). Find your `chat_id` via `https://api.telegram.org/bot<TOKEN>/getUpdates`. Group IDs usually start with `-100`.
3. Edit `config.yaml`:

   ```yaml
   telegram:
     enabled: true
     bot_token: "YOUR_BOT_TOKEN"   # or use the FUSETRACE_TG_TOKEN env var
     chat_id: "YOUR_CHAT_ID"       # or use the FUSETRACE_TG_CHAT_ID env var
   ```

4. Send a test message:

   ```bash
   python fusetrace.py --test-telegram
   ```

Alerts are sent when a cascade **opens** and when it **closes** (or is interrupted). The detector never waits on the network: Telegram runs in a background thread with retries, rate-limit handling and a plain-text fallback if Telegram rejects the formatting. The *opening* alert waits at most 4 seconds for precursor evidence; if Binance is slow, it goes out without it and the evidence is added to the database and the closing message.

---

## 🧭 Usage

| Command | What it does |
|---|---|
| `python fusetrace.py` | **Live monitoring.** Connects to Binance, detects cascades, stores events, sends alerts. Runs until you press `Ctrl+C`. |
| `python fusetrace.py --simulate` | Runs a **synthetic cascade** through the full pipeline. Writes to a separate file, `fusetrace_sim.db`, so your real data stays clean. |
| `python fusetrace.py --list-events` | Lists the 20 most recent stored events. |
| `python fusetrace.py --replay <event_id>` | **Offline replay** of a stored event. Accepts a full ID, an ID prefix, or `latest`. |
| `python fusetrace.py --replay <event_id> --rows 0` | Replay and print **every** timeline row (default is 60). |
| `python fusetrace.py --attribute <event_id>` | **Re-run precursor attribution** on a stored event using historical data. Replaces any previous attribution for that event. |
| `python fusetrace.py --probe ETHUSDT` | Smoke-tests the three precursor REST endpoints for one symbol. Run this first on a new machine or network. |
| `python fusetrace.py --test-telegram` | Sends a test message to verify your bot token and chat ID. |
| `python fusetrace.py -c other.yaml` | Use a different config file. |
| `python fusetrace.py --db <path> ...` | Override `storage.path` for any command above. |

### Replaying simulated data (you must pass `--db`)

`--simulate` deliberately writes to **`fusetrace_sim.db`**, not to your real database (`fusetrace.db`). Replay and `--list-events` read from the database named in `config.yaml` unless you tell them otherwise, so to inspect the simulated event you **must** point at the simulation file:

```bash
python fusetrace.py --db fusetrace_sim.db --replay latest
```

Without `--db fusetrace_sim.db`, the command looks in `fusetrace.db` and will either report that no event exists or show a *real* event. The same applies to listing:

```bash
python fusetrace.py --db fusetrace_sim.db --list-events
```

### Replaying a real event

```bash
python fusetrace.py --list-events                 # find an event ID
python fusetrace.py --replay latest               # replay the newest one
python fusetrace.py --replay 20261102-0043        # an ID prefix works too
```

Replay never touches the network. The precursor evidence was captured when the event happened and is stored in SQLite, so anyone with the database file can re-check the conclusion.

### Re-running attribution on past events

```bash
python fusetrace.py --attribute 20261102-004311
python fusetrace.py --replay 20261102-004311      # see the updated result
```

- Candidates are rebuilt from the stored timeline (the top symbols by liquidated notional in the first `window_seconds`, plus the first symbol seen).
- Funding uses Binance's **historical** funding endpoint (the last settled rate before the event), not today's rate.
- Open-interest history on Binance only goes back about **30 days**; older events will have price and funding evidence only.
- If every lookup fails (for example, no network), your existing attribution is left untouched.
- Changed your weights or thresholds in `config.yaml`? Re-run `--attribute` to see how the ranking changes.

### Running it in the background

```bash
# Quick and simple
nohup python fusetrace.py > fusetrace.log 2>&1 &

# Or use tmux / screen, or a systemd service on Linux:
```

```ini
# /etc/systemd/system/fusetrace.service
[Unit]
Description=FuseTrace liquidation cascade detector
After=network-online.target

[Service]
WorkingDirectory=/path/to/fusetrace
ExecStart=/path/to/fusetrace/.venv/bin/python fusetrace.py
Environment=FUSETRACE_TG_TOKEN=123456789:AAxxxxxxxx
Environment=FUSETRACE_TG_CHAT_ID=-1001234567890
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

FuseTrace marks any event left open by a crash as `interrupted` on the next start, and sends a final message when you stop it with `Ctrl+C` or `SIGTERM`.

---

## 🔬 How it works

```text
 Binance public WebSocket                 Binance public REST (only on cascade)
 !forceOrder@arr                          klines · openInterestHist · fundingRate/premiumIndex
        │                                               ▲
        ▼                                               │
  Parse & normalize ──► Rolling window (60s) ──► Cascade detector ──► Precursor engine
                        + 60-min baseline              │                    │
                                                       ▼                    ▼
                                              SQLite (WAL) ◄──── Ranked candidates
                                                       │
                       ┌───────────────────────────────┼──────────────────┐
                       ▼                               ▼                  ▼
                  Telegram alert                  --replay           --attribute
```

### 1. Cascade detection

A cascade opens when **all** of these hold:

1. Total liquidated notional in the rolling window ≥ `min_total_usd`.
2. At least `min_symbols` different symbols are liquidating at once.
3. The window total is at least `baseline_multiplier`× the average for an equally long window over the previous hour (the most recent window is excluded so the burst can't inflate its own baseline). For the first 10 minutes after start-up the baseline is still warming up and only the absolute threshold applies.

It closes once the window falls below `end_ratio` × the threshold for `quiet_seconds`.

### 2. Precursor attribution

At trigger time, FuseTrace picks candidates: the top `top_k` symbols by liquidated notional in the window, plus the first symbol seen. For each one it asks Binance's public REST API what happened in the `lookback_minutes` (default 15) **before the first liquidation**:

| Evidence | Endpoint | Meaning |
|---|---|---|
| **Price move** | `GET /fapi/v1/klines` (1m) | Did price already move toward the side being liquidated? |
| **Open-interest build-up** | `GET /futures/data/openInterestHist` (5m) | Was leverage piling up? Measured in coin units so a falling price doesn't fake a drop. |
| **Funding rate** | `GET /fapi/v1/premiumIndex` (live) / `GET /fapi/v1/fundingRate` (historical) | Was the crowded side paying up? |

Each piece is scaled to 0–1 and combined with configurable weights:

```text
score = 0.5 × price_stress + 0.3 × oi_buildup + 0.2 × funding_crowding
```

- **Direction-aware.** If longs are being liquidated, stress means price falling and funding positive (crowded longs). If shorts are being liquidated, it's the reverse. OI build-up counts in both cases.
- **Missing evidence counts as zero.** No data never earns points.
- **Honest fallback.** If the best score is below `min_score` (default 0.25), FuseTrace does **not** name an ignition point. It reports the first symbol seen and says there were no clear precursors.

The score is a transparent heuristic, **not a probability.** The weights and scales are starting points; tune them in `config.yaml` and use `--attribute` to see how past events re-rank.

### 3. Storage

One SQLite file in WAL mode:

| Table | Content |
|---|---|
| `liquidations` | Raw liquidation stream (optional, auto-pruned) |
| `events` | One summary row per cascade |
| `event_timeline` | Every liquidation inside an event |
| `event_precursors` | Ranked precursor evidence per candidate symbol |

---

## 📡 Data sources

Everything is **public, free, and keyless.**

| Source | Used for |
|---|---|
| `wss://fstream.binance.com/market/ws/!forceOrder@arr` | Real-time liquidations (USDⓈ-M Futures) |
| `GET https://fapi.binance.com/fapi/v1/klines` | Price history |
| `GET https://fapi.binance.com/futures/data/openInterestHist` | Open-interest history |
| `GET https://fapi.binance.com/fapi/v1/premiumIndex` | Latest funding rate (live mode) |
| `GET https://fapi.binance.com/fapi/v1/fundingRate` | Funding-rate history (`--attribute`) |
| Telegram Bot API | Alerts (optional) |

> **Heads-up on the WebSocket URL.** In 2026 Binance split its USDⓈ-M WebSocket service into `/public`, `/market` and `/private` paths, and `!forceOrder@arr` lives under **`/market`**. The old `wss://fstream.binance.com/ws/...` path no longer delivers this stream. If you have an old config, FuseTrace rewrites the legacy URL automatically and logs a warning, and it warns if no liquidation message arrives for 5 minutes. Binance changes endpoints from time to time; if something stops working, check the official docs first.

---

## ⚙️ Configuration reference

<details>
<summary><b>Click to expand the key options in <code>config.yaml</code></b></summary>

```yaml
source:
  ws_url: "wss://fstream.binance.com/market/ws/!forceOrder@arr"
  rest_url: "https://fapi.binance.com"

detection:
  window_seconds: 60         # rolling window length
  min_total_usd: 10_000_000  # lower this (e.g. 500_000) to test more easily
  min_symbols: 3             # a "cascade" needs several symbols, not one wick
  baseline_minutes: 60
  min_baseline_minutes: 10   # warm-up before the baseline ratio is enforced
  baseline_multiplier: 5.0
  end_ratio: 0.5
  quiet_seconds: 30
  max_timeline_rows: 5000    # per event; summary totals are always complete

precursor:
  enabled: true
  lookback_minutes: 15
  top_k: 3
  include_first_seen: true
  max_workers: 3             # concurrent REST requests (kept low on purpose)
  request_timeout: 4
  wait_seconds: 4            # how long the opening alert waits for evidence
  cooldown_seconds: 120      # minimum gap between two lookups
  backoff_seconds: 300       # pause after HTTP 429/418
  min_score: 0.25
  price_scale_pct: 2.0       # price move that earns full marks
  oi_scale_pct: 5.0          # OI build-up that earns full marks
  funding_scale_pct: 0.05    # funding rate that earns full marks
  weight_price: 0.5
  weight_oi: 0.3
  weight_funding: 0.2

storage:
  path: fusetrace.db
  retention_days: 30
  store_all_liquidations: true

telegram:
  enabled: false
  bot_token: "YOUR_BOT_TOKEN"
  chat_id: "YOUR_CHAT_ID"
  notify_on_open: true
  notify_on_close: true
```

</details>

---

## ⚠️ Honest limitations

We would rather you trust FuseTrace than be misled by it.

- **Binance USDⓈ-M only (for now).** Other exchanges are not implemented.
- **The liquidation stream is a sample, not a full tape.** Binance pushes only the single largest liquidation per symbol per 1000 ms, so in violent markets **total liquidated size is under-counted.** Tune `min_total_usd` against what you actually observe.
- **No account identity.** Centralized-exchange liquidations don't reveal who was liquidated. FuseTrace attributes *markets* (which symbol showed stress first), never individuals.
- **Attribution is a statistical heuristic.** The score is explainable but not validated as a probability. Correlation in pre-event data is not proof of cause.
- **Precursor data has limited resolution.** Open interest comes in 5-minute buckets and is published with some delay, so the OI window can differ slightly from the nominal 15 minutes. The actual span is printed next to each value (for example `OI +4.4%/10m`).
- **History limits.** Binance only serves ~30 days of open-interest history, which caps how far back `--attribute` can see OI.
- **Warm-up.** The baseline isn't persisted across restarts; after launching, the ratio check is relaxed for about 10 minutes.
- **Network access to Binance is required.** Some regions and hosting providers are blocked by Binance; run `--probe` to check.
- **Not financial advice.** FuseTrace analyzes public market data. Use at your own risk.

---

## 🛠️ Troubleshooting

| Symptom | What to check |
|---|---|
| `no liquidation messages for 300s` warning | Check `source.ws_url`. It must use the **`/market`** path (see the note above). |
| `--probe` reports `HTTP 429/418` | You've been rate-limited. FuseTrace pauses lookups automatically; wait and retry. |
| `--probe` reports `URLError` / `HTTP 451` | Network problem or region restriction. Try another network or host. |
| Telegram message not received | Run `--test-telegram`. Make sure you messaged the bot first; group `chat_id`s start with `-100`. |
| `--replay latest` says no event / shows the wrong event | You probably need `--db`. Simulated events live in `fusetrace_sim.db` (see [above](#replaying-simulated-data-you-must-pass---db)). |
| Cascades never trigger | Your thresholds may be too high for current volatility. Lower `min_total_usd` temporarily. |
| `--replay` says "none recorded" for precursors | The event predates the feature, or lookups were off/failed. Run `--attribute <event_id>`. |

---

## 📁 Project layout

```text
fusetrace/
├── fusetrace.py          # live monitor, detector, precursor engine, Telegram, replay, CLI
├── database.py           # SQLite schema (WAL) and queries
├── config.example.yaml   # template: copy to config.yaml and fill in
├── config.yaml           # YOUR private config (git-ignored, never commit)
└── fusetrace.db          # your local event database (git-ignored)
```

---

## 🗺️ Roadmap (ideas, not promises)

- [ ] Batch mode: `--attribute all` with built-in rate-limit protection
- [ ] Persist the baseline across restarts
- [ ] More exchanges (Bybit, OKX) behind the same adapter interface
- [ ] On-chain perpetual venues for address-level attribution (experimental)
- [ ] "Which past event does this look like?" similarity search
- [ ] Published benchmarks on old laptops / Raspberry Pi
- [ ] Optional local summary generation (never required)

---

## 🤝 Contributing

FuseTrace gets better with real crashes and real hardware.

- ⭐ **Star the repo** so other traders can find it.
- 🐛 **Open an issue** with an event where the attribution looked wrong. Include the output of `--replay <event_id>` (the database file is not needed). These become regression cases.
- 🔧 **Send a PR**: new exchange adapters, better scoring rules, tests, translations.
- 📊 **Share a benchmark** from your oldest machine. "It runs on my 2013 laptop" is the best proof of the project's promise.
- 💬 **Join the community:** `https://t.me/<your_group>`

Please never paste tokens, chat IDs or your `config.yaml` into issues.

---

## 📜 License

MIT © `<your-name>`. See [LICENSE](LICENSE).

> **Disclaimer:** FuseTrace is provided for research and informational purposes only. It does not constitute investment advice. Trading derivatives involves substantial risk of loss. You are solely responsible for your own decisions.

<div align="center">

*Built by a trader, for traders, on hardware you already own.*

</div>
