# ── Orderbook depth ───────────────────────────────────────────────────────────
# How many price levels to pull per side in REST snapshots (used on connect
# and reconnect). Higher = more book depth but slower snapshot fetches.
ORDERBOOK_DEPTH = 10

# ── Reconnect / backoff ───────────────────────────────────────────────────────
# Seconds to wait before the first reconnect attempt.
BACKOFF_INITIAL_S = 1.0

# Backoff doubles each failed attempt up to this ceiling (seconds).
BACKOFF_MAX_S = 60.0

# If a connection stays alive at least this many seconds before dropping,
# treat it as a "clean" session and reset the backoff counter to BACKOFF_INITIAL_S.
BACKOFF_RESET_AFTER_S = 30.0

# ── Endpoints ─────────────────────────────────────────────────────────────────
# Switch KALSHI_ENV to "demo" to hit the demo environment instead of prod.
KALSHI_ENV = "prod"  # "prod" | "demo"

_KALSHI_ENDPOINTS = {
    "prod": {
        "ws":   "wss://api.elections.kalshi.com/trade-api/ws/v2",
        "rest": "https://api.elections.kalshi.com/trade-api/v2",
    },
    "demo": {
        "ws":   "wss://demo-api.kalshi.co/trade-api/ws/v2",
        "rest": "https://demo-api.kalshi.co/trade-api/v2",
    },
}

KALSHI_WS_URI   = _KALSHI_ENDPOINTS[KALSHI_ENV]["ws"]
KALSHI_REST_BASE = _KALSHI_ENDPOINTS[KALSHI_ENV]["rest"]

POLY_WS_URI   = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
POLY_REST_BASE = "https://clob.polymarket.com"

# ── Logging ───────────────────────────────────────────────────────────────────
# Standard Python log levels: DEBUG, INFO, WARNING, ERROR
LOG_LEVEL = "INFO"
