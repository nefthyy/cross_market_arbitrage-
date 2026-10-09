# crossMarketArb

An asynchronous cross-venue arbitrage engine for binary prediction markets. It keeps local L2 order books for
[Kalshi](https://kalshi.com) and [Polymarket](https://polymarket.com) in sync over WebSocket, evaluates
fee-adjusted arbitrage conditions on mapped market pairs, gates candidates through a pre-trade risk layer, and
routes them to either a paper-trading ledger or live order placement on both venues.

## Overview

Two economically equivalent contracts listed on different venues should satisfy a no-arbitrage bound: the cost
of acquiring a complete set of mutually exclusive outcomes cannot be below the $1.00 settlement payout. The engine
looks for violations of that bound once taker fees are included.

Two pair topologies are supported:

| Type        | Construction                                                                                   | Condition                                                  |
|-------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------|
| `binary`    | One Kalshi market vs. one Polymarket YES/NO token pair. Buy YES on one venue, NO on the other. | `1 − ask_K − ask_P − fee_K − fee_P ≥ min_profit_per_contract` |
| `partition` | An N-way exhaustive outcome set (e.g. FOMC rate decision buckets). For each outcome, buy YES on whichever venue has the lower all-in cost. | `1 − Σᵢ (askᵢ + feeᵢ) ≥ min_profit_per_contract` |

For `binary` pairs both directions (`k_yes_p_no`, `k_no_p_yes`) are evaluated and the one with the higher net edge
per contract is selected. Trade size is the minimum of top-of-book size across all legs, capped at
`max_contracts_per_trade`.

## Architecture

```
                    ┌──────────────────┐        ┌──────────────────────┐
  Kalshi WS v2 ───► │   kalshi_feed    │        │   polymarket_feed    │ ◄─── Polymarket CLOB WS
  Kalshi REST  ───► │ (RSA-PSS signed) │        │                      │ ◄─── Polymarket CLOB REST
                    └────────┬─────────┘        └──────────┬───────────┘
                             │   writes                    │   writes
                             ▼                             ▼
                    ┌─────────────────────────────────────────────────┐
                    │ books = {"kalshi": {ticker: {yes, no}},         │
                    │          "poly":   {token:  {bids, asks}}}      │
                    └────────────────────────┬────────────────────────┘
                                             │ reads (100 ms cadence)
                                             ▼
                    ┌─────────────────────────────────────────────────┐
                    │ scan_loop → net_profit_{binary,partition}       │
                    │           → check_risk → execute                │
                    └───────────────┬───────────────────┬─────────────┘
                                    │ mode=paper        │ mode=live
                                    ▼                   ▼
                          paper_trades.json     orders.execute_live
                            (JSON Lines)        (concurrent FOK legs)
```

All three components run as tasks in a single `asyncio.TaskGroup` and share the `books` and `state` dicts in
process memory. Because the event loop is single-threaded and book mutations contain no `await` points, no
explicit locking is required.

### Modules

| File                 | Responsibility |
|----------------------|----------------|
| `arb_bot/main.py`    | Entry point. Loads `.env`, `pairs.json`, `config.json`; initializes shared state; starts the task group. |
| `arb_bot/feeds.py`   | Market data. Pair loading, Kalshi request signing, WebSocket subscription, REST snapshotting, delta application, sequence-gap detection, reconnect with exponential backoff. |
| `arb_bot/arb.py`     | Strategy. Fee models, opportunity detection for both pair types, risk checks, paper execution, scan loop. |
| `arb_bot/orders.py`  | Live execution. Signed Kalshi order REST calls and Polymarket CLOB orders via `py-clob-client`. Only imported when `mode != "paper"`. |
| `arb_bot/config.py`  | Transport constants: endpoints, book depth, backoff parameters, log level. |
| `arb_bot/config.json`| Strategy configuration: mode, fees, execution limits, risk, paper ledger. |
| `arb_bot/pairs.json` | Cross-venue instrument mapping. |

### Market data pipeline

Each feed follows a subscribe-then-snapshot pattern to avoid missing updates during initialization:

1. Open the WebSocket and send the subscription for all instruments.
2. Concurrently fetch a REST order book snapshot for each instrument while buffering inbound WS frames.
3. Once snapshots are applied as the baseline, replay the buffered frames, then enter the steady-state receive loop.

Book representation:

- Prices are normalized to **integer cents** (`_cents`) to avoid floating-point dictionary keys.
- Kalshi books are stored as `{"yes": {price: size}, "no": {price: size}}`; Polymarket as `{"bids": ..., "asks": ...}` per outcome token.
- Kalshi `orderbook_delta` messages are applied incrementally (`size += delta`, level removed at `≤ 0`). Per-ticker
  `seq` is tracked; a gap triggers a fresh REST snapshot for that ticker.
- Polymarket `book` events replace the token's book; `price_change` events set absolute level sizes (level removed at `0`).

Reconnection uses exponential backoff from `BACKOFF_INITIAL_S` doubling to `BACKOFF_MAX_S`. If a session survived
at least `BACKOFF_RESET_AFTER_S` before dropping, the backoff resets.

### Fee models

**Kalshi** (per contract, price `p` in dollars):

```
fee = min(ceil_to_cent(coeff · p · (1 − p)), fee_cap_per_contract)
```

`coeff` is `taker_coefficient` (0.07) or `maker_coefficient` (0.0175), scaled by `sp500_nasdaq_multiplier` for
`KXSPX`/`KXNDX` tickers.

**Polymarket** (per contract):

```
fee = taker_rate[category] · p · (1 − p)        (maker = 0)
```

The category rate is taken from `fees.polymarket.categories[pair.poly_category]`, falling back to `default`.

### Risk controls

`check_risk` is evaluated before every execution and blocks the trade if any of the following hold:

| Control                  | Source                               |
|--------------------------|--------------------------------------|
| Kill switch              | A file named by `risk.kill_switch_file` exists in the working directory |
| Daily loss limit         | `state.daily_loss ≥ execution.max_daily_loss` |
| Per-pair exposure limit  | `state.pair_exposure[pair] ≥ execution.max_exposure_per_pair` |
| Aggregate exposure limit | `state.total_exposure ≥ execution.max_total_exposure` |

Exposure is accumulated as `contracts × Σ leg ask` on each execution. State is in-memory and resets on process restart.

To halt trading without stopping the process:

```bash
touch arb_bot/KILL_SWITCH      # block all new executions
rm arb_bot/KILL_SWITCH         # resume
```

### Execution

**Paper mode** (`"mode": "paper"`): assumes all legs fill at the observed top-of-book ask, updates exposure and
cumulative PnL, and appends one JSON object per trade to `paper.log_file` (JSON Lines format).

**Live mode** (any other `mode` value, e.g. `"live"`): all legs are submitted concurrently with `asyncio.gather`
under a `risk.max_leg_fill_wait_seconds` timeout.

- Kalshi: `POST /portfolio/orders`, limit order, `time_in_force: fill_or_kill`, price expressed as `yes_price`
  in cents (converted from the NO price for NO-side orders), RSA-PSS/SHA-256 signed headers.
- Polymarket: `ClobClient.create_and_post_order` with `OrderType.FOK`, BUY side, on Polygon (chain id 137).

If any leg returns an error, a `CRITICAL` partial-fill event is logged.

## Requirements

- Python **3.11+** (uses `asyncio.TaskGroup` and PEP 604 unions)
- Kalshi API key (key ID + RSA private key in PEM format)
- For live trading on Polymarket: a Polygon wallet private key and funder (proxy) address with USDC

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Configuration

### Environment variables

Loaded from a `.env` file in the working directory via `python-dotenv` (`.env` is git-ignored).

| Variable                   | Required for       | Description |
|----------------------------|--------------------|-------------|
| `KALSHI_KEY_ID`            | all modes          | Kalshi API key ID |
| `KALSHI_PRIVATE_KEY_PATH`  | all modes          | Path to the PEM-encoded RSA private key used for request signing |
| `POLY_PRIVATE_KEY`         | live mode          | Polygon wallet private key used by `py-clob-client` |
| `POLY_FUNDER_ADDRESS`      | live mode          | Polymarket funder / proxy wallet address |

```dotenv
KALSHI_KEY_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
KALSHI_PRIVATE_KEY_PATH=/path/to/kalshi_private_key.pem
POLY_PRIVATE_KEY=0x...
POLY_FUNDER_ADDRESS=0x...
```

### `config.py`

Transport-level constants. Set `KALSHI_ENV = "demo"` to target Kalshi's demo environment
(`demo-api.kalshi.co`). Polymarket has no demo environment; use paper mode instead.

### `config.json`

| Key                                      | Description |
|------------------------------------------|-------------|
| `mode`                                   | `"paper"` or `"live"` |
| `fees.kalshi.*`                          | Kalshi taker/maker coefficients, per-contract fee cap, index-market multiplier |
| `fees.polymarket.categories.<cat>`       | Taker/maker rates per market category |
| `execution.min_profit_per_contract`      | Minimum net edge per contract, in dollars, after fees |
| `execution.max_contracts_per_trade`      | Upper bound on contracts per execution |
| `execution.max_exposure_per_pair`        | Per-pair notional cap (USD) |
| `execution.max_total_exposure`           | Aggregate notional cap (USD) |
| `execution.max_daily_loss`               | Daily loss circuit breaker (USD) |
| `risk.kill_switch_file`                  | Filename whose presence halts execution |
| `risk.max_leg_fill_wait_seconds`         | Timeout for concurrent leg submission in live mode |
| `risk.unwind_on_partial_fill`            | Flag for partial-fill unwind (see Limitations) |
| `paper.log_file`                         | Output path for the paper trade ledger |

> **Note:** the `feeds` and `log_level` sections of `config.json` are not currently read; the runtime uses the
> equivalent constants in `config.py`. Likewise `execution.order_type`, `execution.simultaneous_legs`,
> `paper.starting_balance` and `fees.polymarket.maker_rebate_daily_usdc` are declared but not referenced.

### `pairs.json`

Maps instruments across venues. Polymarket identifiers are CLOB **token IDs** (one per outcome side), not
condition IDs or slugs.

Binary pair:

```json
{
  "id": "fed-jun-2026",
  "type": "binary",
  "poly_category": "economics",
  "kalshi":     { "ticker": "KXFED-26JUN-T5.25" },
  "polymarket": { "yes_token": "<token_id>", "no_token": "<token_id>" }
}
```

Partition pair: outcomes are aligned across venues by `label`, and the set must be **mutually exclusive and
collectively exhaustive**, otherwise the $1.00 payout assumption does not hold.

```json
{
  "id": "fed-jun-2026-partition",
  "type": "partition",
  "poly_category": "economics",
  "kalshi": {
    "tickers": [
      { "ticker": "KXFED-26JUN-T5.50", "label": "50bp cut" },
      { "ticker": "KXFED-26JUN-T5.25", "label": "25bp cut" }
    ]
  },
  "polymarket": {
    "outcomes": [
      { "yes_token": "<token_id>", "no_token": "<token_id>", "label": "50bp cut" },
      { "yes_token": "<token_id>", "no_token": "<token_id>", "label": "25bp cut" }
    ]
  }
}
```

Correct pairing requires that both venues' contracts resolve on identical criteria (source, timing, edge cases).
Resolution-rule mismatch is the dominant risk in cross-venue prediction market arbitrage and is not checked by
the engine.

## Running

`main.py` resolves `pairs.json`, `config.json`, `.env`, the kill switch and the paper ledger relative to the
current working directory, so launch it from `arb_bot/`:

```bash
cd arb_bot
python main.py
```

Sample log output:

```
2026-10-09 16:40:01,234 INFO starting in PAPER mode
2026-10-09 16:40:01,235 INFO Loaded 5 kalshi tickers, 10 poly tokens
2026-10-09 16:40:01,512 INFO kalshi connected (attempt 1)
2026-10-09 16:40:01,601 INFO polymarket connected (attempt 1)
2026-10-09 16:40:04,118 INFO [PAPER] opportunity — pair=fed-jun-2026 net=0.0210/contract contracts=42 expected=0.8820
```

## Limitations

- **Partial-fill unwind is not implemented.** `unwind_on_partial_fill` is a no-op (`TODO` in `orders.py`); a
  one-legged fill leaves a naked directional position.
- **Top-of-book sizing only.** Size is bounded by the best level on each leg; deeper levels are not walked.
- **Paper fills are optimistic.** No latency, queue position, slippage or rejection modelling.
- **In-memory state.** Exposure, daily loss and PnL are not persisted and do not reflect actual venue positions
  or settled contracts. `daily_loss` is only incremented by negative *expected* profit and never resets on a
  day boundary.
- **Exposure is recorded on error.** In live mode, exposure is incremented even when a leg fails.
- **Re-entry throttling** relies solely on exposure limits; the same opportunity can be re-executed every 100 ms
  scan cycle until a limit is hit.
- **Kalshi signing** reloads the PEM key from disk on every signed request.
- Example partition token IDs in `pairs.json` are placeholders.

## Disclaimer

This software places real orders with real funds when not in paper mode. It is provided for research purposes
without warranty. Verify venue terms of service and the regulatory status of prediction market trading in your
jurisdiction before use.
