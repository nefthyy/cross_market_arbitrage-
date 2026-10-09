"""
Real order placement for Kalshi and Polymarket.
This module is never imported or called when cfg["mode"] == "paper".
"""
import asyncio
import base64
import logging
import os
import time
from uuid import uuid4

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config


def _kalshi_order_auth_headers(method: str, path: str) -> dict:
    key_id = os.environ["KALSHI_KEY_ID"]
    key_path = os.environ["KALSHI_PRIVATE_KEY_PATH"]

    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)

    ts = str(int(time.time() * 1000))
    message = (ts + method + path).encode()
    sig = private_key.sign(
        message,
        padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.MAX_LENGTH),
        hashes.SHA256(),
    )

    return {
        "KALSHI-ACCESS-KEY": key_id,
        "KALSHI-ACCESS-TIMESTAMP": ts,
        "KALSHI-ACCESS-SIGNATURE": base64.b64encode(sig).decode(),
        "Content-Type": "application/json",
    }


async def place_kalshi_order(ticker: str, side: str, price: float, count: int, cfg: dict) -> dict:
    path = "/trade-api/v2/portfolio/orders"
    url = f"{config.KALSHI_REST_BASE}{path}"
    headers = _kalshi_order_auth_headers("POST", path)

    yes_price = int(round(price * 100))
    if side == "no":
        yes_price = 100 - yes_price

    body = {
        "ticker": ticker,
        "side": side,
        "type": "limit",
        "yes_price": yes_price,
        "count": count,
        "time_in_force": "fill_or_kill",
        "client_order_id": str(uuid4()),
    }

    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=body, headers=headers) as resp:
                result = await resp.json()
        order_id = result.get("order", {}).get("order_id", "unknown")
        logging.info(f"[LIVE] kalshi order placed — id={order_id} ticker={ticker} side={side} price={price} count={count}")
        return result
    except Exception as e:
        logging.error(f"[LIVE] kalshi order error — ticker={ticker} side={side}: {e}")
        return {"error": str(e)}


async def place_poly_order(token_id: str, price: float, size: float, cfg: dict) -> dict:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import OrderArgs, OrderType

        private_key = os.environ["POLY_PRIVATE_KEY"]
        funder_address = os.environ["POLY_FUNDER_ADDRESS"]

        client = ClobClient(
            host=config.POLY_REST_BASE,
            key=private_key,
            chain_id=137,
            funder=funder_address,
        )

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side="BUY",
        )
        result = client.create_and_post_order(order_args, OrderType.FOK)
        order_id = result.get("orderID", "unknown") if isinstance(result, dict) else "unknown"
        logging.info(f"[LIVE] polymarket order placed — id={order_id} token={token_id[:16]}... price={price} size={size}")
        return result if isinstance(result, dict) else {"result": str(result)}
    except Exception as e:
        logging.error(f"[LIVE] polymarket order error — token={token_id[:16]}...: {e}")
        return {"error": str(e)}


async def execute_live(opp: dict, cfg: dict, state: dict) -> None:
    pair_id = opp["pair_id"]

    if opp["type"] == "binary":
        try:
            k_result, p_result = await asyncio.wait_for(
                asyncio.gather(
                    place_kalshi_order(opp["k_ticker"], opp["k_side"], opp["k_ask"], opp["max_contracts"], cfg),
                    place_poly_order(opp["p_token"], opp["p_ask"], opp["max_contracts"], cfg),
                    return_exceptions=True,
                ),
                timeout=cfg["risk"]["max_leg_fill_wait_seconds"],
            )
            results = [k_result, p_result]
        except asyncio.TimeoutError:
            logging.error(f"[LIVE] execution timed out for pair {pair_id}")
            return

    else:  # partition
        order_coros = []
        for leg in opp["legs"]:
            if leg["platform"] == "kalshi":
                order_coros.append(
                    place_kalshi_order(leg["ticker_or_token"], "yes", leg["ask"], opp["max_contracts"], cfg)
                )
            else:
                order_coros.append(
                    place_poly_order(leg["ticker_or_token"], leg["ask"], opp["max_contracts"], cfg)
                )
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*order_coros, return_exceptions=True),
                timeout=cfg["risk"]["max_leg_fill_wait_seconds"],
            )
        except asyncio.TimeoutError:
            logging.error(f"[LIVE] execution timed out for pair {pair_id}")
            return

    has_error = any(isinstance(r, dict) and "error" in r for r in results)
    if has_error:
        statuses = [r.get("error", "ok") if isinstance(r, dict) else str(r) for r in results]
        logging.critical(
            f"[LIVE] partial fill detected for pair {pair_id} — statuses: {statuses}"
        )
        if cfg["risk"]["unwind_on_partial_fill"]:
            # TODO: implement unwind
            pass

    exposure_delta = opp["max_contracts"] * opp.get("total_cost", opp.get("k_ask", 0.0) + opp.get("p_ask", 0.0))
    state["total_exposure"] += exposure_delta
    state["pair_exposure"][pair_id] = state["pair_exposure"].get(pair_id, 0.0) + exposure_delta

    logging.info(
        f"[LIVE] executed pair={pair_id} contracts={opp['max_contracts']} "
        f"expected_profit={opp['expected_profit']:.4f} results={results}"
    )
