import asyncio
import base64
import json
import logging
import os
import time

import aiohttp
import websockets
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config


def _cents(price) -> int:
    """Convert a price string/float to integer cents to avoid float-key issues."""
    return int(round(float(price) * 100))


def load_pairs(path: str) -> dict:
    with open(path) as f:
        data = json.load(f)

    kalshi_tickers = []
    poly_tokens = []

    for pair in data["pairs"]:
        if pair["type"] == "binary":
            kalshi_tickers.append(pair["kalshi"]["ticker"])
            poly_tokens.append(pair["polymarket"]["yes_token"])
            poly_tokens.append(pair["polymarket"]["no_token"])
        elif pair["type"] == "partition":
            for entry in pair["kalshi"]["tickers"]:
                kalshi_tickers.append(entry["ticker"])
            for outcome in pair["polymarket"]["outcomes"]:
                poly_tokens.append(outcome["yes_token"])
                poly_tokens.append(outcome["no_token"])

    logging.info(f"Loaded {len(kalshi_tickers)} kalshi tickers, {len(poly_tokens)} poly tokens")
    return {"pairs": data["pairs"], "kalshi_tickers": kalshi_tickers, "poly_tokens": poly_tokens}


def _kalshi_auth_headers() -> dict:
    key_id = os.environ["KALSHI_KEY_ID"]
    key_path = os.environ["KALSHI_PRIVATE_KEY_PATH"]

    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    ts = str(int(time.time() * 1000))
    message = (ts + "GET" + "/trade-api/ws/v2").encode()
    sig = private_key.sign(message, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH), hashes.SHA256())

    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
    }


async def _kalshi_rest_snapshot(tickers: list[str], books: dict, session: aiohttp.ClientSession) -> None:
    for ticker in tickers:
        try:
            url = f"{config.KALSHI_REST_BASE}/markets/{ticker}/orderbook?depth={config.ORDERBOOK_DEPTH}"
            async with session.get(url) as resp:
                data = await resp.json()
            ob = data.get("orderbook", {})
            yes_book = {}
            no_book = {}
            for price_str, size_str in (ob.get("yes_dollars_fp") or []):
                yes_book[_cents(price_str)] = float(size_str)
            for price_str, size_str in (ob.get("no_dollars_fp") or []):
                no_book[_cents(price_str)] = float(size_str)
            books["kalshi"][ticker] = {"yes": yes_book, "no": no_book}
        except Exception as e:
            logging.warning(f"kalshi REST snapshot failed for {ticker}: {e}")


def _process_kalshi_msg(raw, books: dict, seq_map: dict[str, int]) -> str | None:
    """Process a single Kalshi WS message. Returns ticker needing re-snapshot on seq gap, else None."""
    try:
        msg = json.loads(raw)
    except Exception as e:
        logging.warning(f"kalshi JSON parse error: {e}")
        return None

    msg_type = msg.get("type")
    if msg_type not in ("orderbook_snapshot", "orderbook_delta"):
        return None

    try:
        ticker = msg["msg"]["market_ticker"]
        seq = msg.get("seq", 0)

        if msg_type == "orderbook_snapshot":
            yes_book = {}
            no_book = {}
            for price_str, size_str in (msg["msg"].get("yes_dollars_fp") or []):
                yes_book[_cents(price_str)] = float(size_str)
            for price_str, size_str in (msg["msg"].get("no_dollars_fp") or []):
                no_book[_cents(price_str)] = float(size_str)
            books["kalshi"][ticker] = {"yes": yes_book, "no": no_book}
            seq_map[ticker] = seq

        elif msg_type == "orderbook_delta":
            prev_seq = seq_map.get(ticker)
            if prev_seq is not None and seq != prev_seq + 1:
                logging.warning(f"kalshi seq gap on {ticker}: expected {prev_seq + 1}, got {seq}")
                seq_map[ticker] = seq
                return ticker
            seq_map[ticker] = seq

            if ticker not in books["kalshi"]:
                books["kalshi"][ticker] = {"yes": {}, "no": {}}

            side = msg["msg"]["side"]
            if side not in ("yes", "no"):
                logging.warning(f"kalshi unexpected side '{side}' for {ticker}")
                return None
            price = _cents(msg["msg"]["price_dollars"])
            delta = float(msg["msg"]["delta_fp"])
            book_side = books["kalshi"][ticker][side]
            new_size = book_side.get(price, 0.0) + delta
            if new_size <= 0:
                book_side.pop(price, None)
            else:
                book_side[price] = new_size

    except Exception as e:
        logging.warning(f"kalshi message processing error: {e} — raw: {str(raw)[:200]}")

    return None


async def kalshi_feed(tickers: list[str], books: dict) -> None:
    uri = config.KALSHI_WS_URI
    backoff = config.BACKOFF_INITIAL_S
    attempt = 0
    seq_map: dict[str, int] = {}

    async with aiohttp.ClientSession() as session:
        while True:
            attempt += 1
            logging.info(f"kalshi connecting (attempt {attempt})")
            connect_time = None
            try:
                headers = _kalshi_auth_headers()
                async with websockets.connect(uri, extra_headers=headers) as ws:
                    connect_time = time.monotonic()
                    logging.info(f"kalshi connected (attempt {attempt})")

                    # Subscribe FIRST so deltas arriving during snapshot are captured
                    sub_msg = {
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {"channels": ["orderbook_delta"], "market_tickers": tickers},
                    }
                    await ws.send(json.dumps(sub_msg))

                    # Buffer WS messages while taking REST snapshot
                    buffer: list[str] = []
                    snapshot_done = asyncio.Event()

                    async def _do_snapshot():
                        await _kalshi_rest_snapshot(tickers, books, session)
                        snapshot_done.set()

                    snapshot_task = asyncio.create_task(_do_snapshot())

                    try:
                        while not snapshot_done.is_set():
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                                buffer.append(raw)
                            except asyncio.TimeoutError:
                                continue
                        await snapshot_task
                    except Exception:
                        snapshot_task.cancel()
                        raise

                    # Replay buffered deltas (snapshot already applied as baseline)
                    for raw in buffer:
                        resnapshot_ticker = _process_kalshi_msg(raw, books, seq_map)
                        if resnapshot_ticker:
                            await _kalshi_rest_snapshot([resnapshot_ticker], books, session)
                            seq_map.pop(resnapshot_ticker, None)

                    # Normal message loop
                    async for raw in ws:
                        resnapshot_ticker = _process_kalshi_msg(raw, books, seq_map)
                        if resnapshot_ticker:
                            await _kalshi_rest_snapshot([resnapshot_ticker], books, session)
                            seq_map.pop(resnapshot_ticker, None)

            except Exception as e:
                logging.warning(f"kalshi disconnected (attempt {attempt}): {e}")

            elapsed = time.monotonic() - connect_time if connect_time else 0
            if elapsed >= config.BACKOFF_RESET_AFTER_S:
                backoff = config.BACKOFF_INITIAL_S
                attempt = 0
            else:
                backoff = min(backoff * 2, config.BACKOFF_MAX_S)

            logging.info(f"kalshi reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)


def _process_poly_msg(msg: dict, books: dict) -> None:
    """Process a single Polymarket WS message."""
    token_id = msg.get("asset_id")
    event_type = msg.get("event_type")

    if event_type == "book":
        bids = {}
        asks = {}
        for item in (msg.get("bids") or []):
            bids[_cents(item["price"])] = float(item["size"])
        for item in (msg.get("asks") or []):
            asks[_cents(item["price"])] = float(item["size"])
        books["poly"][token_id] = {"bids": bids, "asks": asks}

    elif event_type == "price_change":
        if token_id not in books["poly"]:
            books["poly"][token_id] = {"bids": {}, "asks": {}}
        for change in (msg.get("changes") or []):
            side_raw = change.get("side")
            side = "bids" if side_raw == "BUY" else "asks"
            price = _cents(change["price"])
            size = float(change["size"])
            if size == 0.0:
                books["poly"][token_id][side].pop(price, None)
            else:
                books["poly"][token_id][side][price] = size


async def _poly_rest_snapshot(tokens: list[str], books: dict, session: aiohttp.ClientSession) -> None:
    for token_id in tokens:
        try:
            url = f"{config.POLY_REST_BASE}/book?token_id={token_id}"
            async with session.get(url) as resp:
                data = await resp.json()
            bids = {}
            asks = {}
            for item in (data.get("bids") or []):
                bids[_cents(item["price"])] = float(item["size"])
            for item in (data.get("asks") or []):
                asks[_cents(item["price"])] = float(item["size"])
            books["poly"][token_id] = {"bids": bids, "asks": asks}
        except Exception as e:
            logging.warning(f"polymarket REST snapshot failed for {token_id}: {e}")


async def polymarket_feed(tokens: list[str], books: dict) -> None:
    uri = config.POLY_WS_URI
    backoff = config.BACKOFF_INITIAL_S
    attempt = 0

    async with aiohttp.ClientSession() as session:
        while True:
            attempt += 1
            logging.info(f"polymarket connecting (attempt {attempt})")
            connect_time = None
            try:
                async with websockets.connect(uri) as ws:
                    connect_time = time.monotonic()
                    logging.info(f"polymarket connected (attempt {attempt})")

                    # Subscribe FIRST so deltas arriving during snapshot are captured
                    sub_msg = {
                        "assets_ids": tokens,
                        "type": "market",
                        "custom_feature_enabled": True,
                    }
                    await ws.send(json.dumps(sub_msg))

                    # Buffer WS messages while taking REST snapshot
                    buffer: list[str] = []
                    snapshot_done = asyncio.Event()

                    async def _do_snapshot():
                        await _poly_rest_snapshot(tokens, books, session)
                        snapshot_done.set()

                    snapshot_task = asyncio.create_task(_do_snapshot())

                    try:
                        while not snapshot_done.is_set():
                            try:
                                raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                                buffer.append(raw)
                            except asyncio.TimeoutError:
                                continue
                        await snapshot_task
                    except Exception:
                        snapshot_task.cancel()
                        raise

                    # Replay buffered messages
                    for raw in buffer:
                        try:
                            msgs = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for msg in msgs:
                            try:
                                _process_poly_msg(msg, books)
                            except Exception as e:
                                logging.warning(f"polymarket buffer replay error: {e}")

                    # Normal message loop
                    async for raw in ws:
                        try:
                            msgs = json.loads(raw)
                        except Exception as e:
                            logging.warning(f"polymarket JSON parse error: {e}")
                            continue

                        if not isinstance(msgs, list):
                            msgs = [msgs]

                        for msg in msgs:
                            try:
                                _process_poly_msg(msg, books)
                            except Exception as e:
                                token_id = msg.get("asset_id")
                                logging.warning(f"polymarket message processing error for {token_id}: {e}")

            except Exception as e:
                logging.warning(f"polymarket disconnected (attempt {attempt}): {e}")

            elapsed = time.monotonic() - connect_time if connect_time else 0
            if elapsed >= config.BACKOFF_RESET_AFTER_S:
                backoff = config.BACKOFF_INITIAL_S
                attempt = 0
            else:
                backoff = min(backoff * 2, config.BACKOFF_MAX_S)

            logging.info(f"polymarket reconnecting in {backoff:.0f}s")
            await asyncio.sleep(backoff)
