"""
Arbitrage detection, execution routing, and paper trade tracking.
"""
import asyncio
import json
import logging
import math
import os
from datetime import datetime


def load_config(path: str) -> dict:
    with open(path) as f:
        cfg = json.load(f)
    mode = cfg["mode"].upper()
    logging.info(f"starting in {mode} mode")
    return cfg


def kalshi_fee(price: float, cfg: dict, ticker: str = "", is_maker: bool = False) -> float:
    k_cfg = cfg["fees"]["kalshi"]
    if is_maker:
        coeff = k_cfg["maker_coefficient"]
    else:
        coeff = k_cfg["taker_coefficient"]

    if "KXSPX" in ticker or "KXNDX" in ticker:
        coeff *= k_cfg["sp500_nasdaq_multiplier"]

    raw = coeff * price * (1.0 - price)
    fee = math.ceil(raw * 100) / 100
    return min(fee, k_cfg["fee_cap_per_contract"])


def poly_fee(price: float, category: str, cfg: dict, is_maker: bool = False) -> float:
    categories = cfg["fees"]["polymarket"]["categories"]
    rates = categories.get(category, categories["default"])
    if is_maker:
        return 0.0
    return rates["taker"] * price * (1.0 - price)


def _available_size_kalshi(book_side: dict, price_cents: int) -> float:
    return book_side.get(price_cents, 0.0)


def _available_size_poly(book_side: dict, price_cents: int) -> float:
    return book_side.get(price_cents, 0.0)


def net_profit_binary(pair: dict, books: dict, cfg: dict) -> dict | None:
    ticker = pair["kalshi"]["ticker"]
    yes_token = pair["polymarket"]["yes_token"]
    no_token = pair["polymarket"]["no_token"]
    category = pair.get("poly_category", "default")
    min_profit = cfg["execution"]["min_profit_per_contract"]
    max_contracts_cfg = cfg["execution"]["max_contracts_per_trade"]

    k_book = books["kalshi"].get(ticker)
    p_yes_book = books["poly"].get(yes_token)
    p_no_book = books["poly"].get(no_token)

    if not k_book or not p_yes_book or not p_no_book:
        return None

    best = None

    # Direction 1: buy YES on Kalshi, buy NO on Polymarket
    if k_book["yes"] and p_no_book["asks"]:
        k_ask_cents = min(k_book["yes"].keys())
        p_ask_cents = min(p_no_book["asks"].keys())
        k_ask = k_ask_cents / 100.0
        p_ask = p_ask_cents / 100.0
        gross = 1.0 - k_ask - p_ask
        k_fee = kalshi_fee(k_ask, cfg, ticker)
        p_fee = poly_fee(p_ask, category, cfg)
        net = gross - k_fee - p_fee
        if net >= min_profit:
            k_size = _available_size_kalshi(k_book["yes"], k_ask_cents)
            p_size = _available_size_poly(p_no_book["asks"], p_ask_cents)
            contracts = min(int(k_size), int(p_size), max_contracts_cfg)
            if contracts > 0:
                best = {
                    "pair_id": pair["id"],
                    "type": "binary",
                    "direction": "k_yes_p_no",
                    "k_ticker": ticker,
                    "k_side": "yes",
                    "k_ask": k_ask,
                    "k_fee": k_fee,
                    "p_token": no_token,
                    "p_ask": p_ask,
                    "p_fee": p_fee,
                    "gross": gross,
                    "net_per_contract": net,
                    "max_contracts": contracts,
                    "expected_profit": net * contracts,
                }

    # Direction 2: buy NO on Kalshi, buy YES on Polymarket
    if k_book["no"] and p_yes_book["asks"]:
        k_ask_cents = min(k_book["no"].keys())
        p_ask_cents = min(p_yes_book["asks"].keys())
        k_ask = k_ask_cents / 100.0
        p_ask = p_ask_cents / 100.0
        gross = 1.0 - k_ask - p_ask
        k_fee = kalshi_fee(k_ask, cfg, ticker)
        p_fee = poly_fee(p_ask, category, cfg)
        net = gross - k_fee - p_fee
        if net >= min_profit:
            k_size = _available_size_kalshi(k_book["no"], k_ask_cents)
            p_size = _available_size_poly(p_yes_book["asks"], p_ask_cents)
            contracts = min(int(k_size), int(p_size), max_contracts_cfg)
            if contracts > 0:
                candidate = {
                    "pair_id": pair["id"],
                    "type": "binary",
                    "direction": "k_no_p_yes",
                    "k_ticker": ticker,
                    "k_side": "no",
                    "k_ask": k_ask,
                    "k_fee": k_fee,
                    "p_token": yes_token,
                    "p_ask": p_ask,
                    "p_fee": p_fee,
                    "gross": gross,
                    "net_per_contract": net,
                    "max_contracts": contracts,
                    "expected_profit": net * contracts,
                }
                if best is None or candidate["net_per_contract"] > best["net_per_contract"]:
                    best = candidate

    return best


def net_profit_partition(pair: dict, books: dict, cfg: dict) -> dict | None:
    category = pair.get("poly_category", "default")
    min_profit = cfg["execution"]["min_profit_per_contract"]
    max_contracts_cfg = cfg["execution"]["max_contracts_per_trade"]

    # Build aligned list of (kalshi_ticker, yes_token, label) from pair
    k_tickers = {entry["label"]: entry["ticker"] for entry in pair["kalshi"]["tickers"]}
    outcomes = pair["polymarket"]["outcomes"]

    legs = []
    min_size = float("inf")

    for outcome in outcomes:
        label = outcome["label"]
        yes_token = outcome["yes_token"]
        k_ticker = k_tickers.get(label)

        if not k_ticker:
            return None

        k_book = books["kalshi"].get(k_ticker)
        p_book = books["poly"].get(yes_token)

        if not k_book or not p_book:
            return None

        k_yes = k_book.get("yes", {})
        p_asks = p_book.get("asks", {})

        k_ask_cents = min(k_yes.keys()) if k_yes else None
        p_ask_cents = min(p_asks.keys()) if p_asks else None

        if k_ask_cents is None and p_ask_cents is None:
            return None

        # Pick the cheaper option after fees
        k_ask = k_ask_cents / 100.0 if k_ask_cents is not None else float("inf")
        p_ask = p_ask_cents / 100.0 if p_ask_cents is not None else float("inf")
        k_fee = kalshi_fee(k_ask, cfg, k_ticker) if k_ask_cents is not None else float("inf")
        p_fee = poly_fee(p_ask, category, cfg) if p_ask_cents is not None else float("inf")

        k_total = k_ask + k_fee
        p_total = p_ask + p_fee

        if k_total <= p_total and k_ask_cents is not None:
            size = _available_size_kalshi(k_yes, k_ask_cents)
            legs.append({
                "outcome_label": label,
                "platform": "kalshi",
                "ticker_or_token": k_ticker,
                "ask": k_ask,
                "fee": k_fee,
            })
        elif p_ask_cents is not None:
            size = _available_size_poly(p_asks, p_ask_cents)
            legs.append({
                "outcome_label": label,
                "platform": "polymarket",
                "ticker_or_token": yes_token,
                "ask": p_ask,
                "fee": p_fee,
            })
        else:
            return None

        min_size = min(min_size, size)

    total_cost = sum(leg["ask"] + leg["fee"] for leg in legs)
    net = 1.0 - total_cost

    if net < min_profit:
        return None

    contracts = min(int(min_size), max_contracts_cfg)
    if contracts <= 0:
        return None

    return {
        "pair_id": pair["id"],
        "type": "partition",
        "legs": legs,
        "total_cost": total_cost,
        "net_per_contract": net,
        "max_contracts": contracts,
        "expected_profit": net * contracts,
    }


def check_risk(opp: dict, state: dict, cfg: dict) -> bool:
    kill_switch = cfg["risk"]["kill_switch_file"]
    if os.path.exists(kill_switch):
        logging.warning(f"[RISK] KILL SWITCH ACTIVE — blocking pair {opp['pair_id']}")
        return False

    if state["daily_loss"] >= cfg["execution"]["max_daily_loss"]:
        logging.warning(
            f"[RISK] daily loss limit reached ({state['daily_loss']:.2f} >= "
            f"{cfg['execution']['max_daily_loss']:.2f}) — blocking pair {opp['pair_id']}"
        )
        return False

    pair_exposure = state["pair_exposure"].get(opp["pair_id"], 0.0)
    if pair_exposure >= cfg["execution"]["max_exposure_per_pair"]:
        logging.warning(
            f"[RISK] per-pair exposure limit reached for {opp['pair_id']} "
            f"({pair_exposure:.2f} >= {cfg['execution']['max_exposure_per_pair']:.2f})"
        )
        return False

    if state["total_exposure"] >= cfg["execution"]["max_total_exposure"]:
        logging.warning(
            f"[RISK] total exposure limit reached "
            f"({state['total_exposure']:.2f} >= {cfg['execution']['max_total_exposure']:.2f})"
        )
        return False

    return True


async def _paper_execute(opp: dict, cfg: dict, state: dict) -> None:
    contracts = opp["max_contracts"]
    expected_profit = opp["expected_profit"]

    if opp["type"] == "binary":
        legs_record = [
            {
                "platform": "kalshi",
                "ticker_or_token": opp["k_ticker"],
                "side": opp["k_side"],
                "price": opp["k_ask"],
                "fee": opp["k_fee"],
            },
            {
                "platform": "polymarket",
                "ticker_or_token": opp["p_token"],
                "side": "buy",
                "price": opp["p_ask"],
                "fee": opp["p_fee"],
            },
        ]
        exposure_delta = contracts * (opp["k_ask"] + opp["p_ask"])
    else:
        legs_record = opp["legs"]
        exposure_delta = contracts * opp["total_cost"]

    state["total_exposure"] += exposure_delta
    state["pair_exposure"][opp["pair_id"]] = (
        state["pair_exposure"].get(opp["pair_id"], 0.0) + exposure_delta
    )
    if expected_profit < 0:
        state["daily_loss"] += abs(expected_profit)

    state["paper_pnl"] += expected_profit

    record = {
        "timestamp": datetime.utcnow().isoformat(),
        "mode": "paper",
        "pair_id": opp["pair_id"],
        "type": opp["type"],
        "direction": opp.get("direction", "partition"),
        "contracts": contracts,
        "expected_profit": expected_profit,
        "net_per_contract": opp["net_per_contract"],
        "legs": legs_record,
        "cumulative_paper_pnl": state["paper_pnl"],
    }

    log_file = cfg["paper"]["log_file"]
    with open(log_file, "a") as f:
        f.write(json.dumps(record) + "\n")

    logging.info(f"[PAPER] {json.dumps(record)}")


async def execute(opp: dict, cfg: dict, state: dict) -> None:
    if cfg["mode"] == "paper":
        await _paper_execute(opp, cfg, state)
        return

    from orders import execute_live
    await execute_live(opp, cfg, state)


async def scan_loop(pairs: list, books: dict, cfg: dict, state: dict) -> None:
    mode_tag = f"[{cfg['mode'].upper()}]"

    while True:
        for pair in pairs:
            try:
                if pair["type"] == "binary":
                    opp = net_profit_binary(pair, books, cfg)
                elif pair["type"] == "partition":
                    opp = net_profit_partition(pair, books, cfg)
                else:
                    continue

                if opp is not None:
                    logging.info(
                        f"{mode_tag} opportunity — pair={opp['pair_id']} "
                        f"net={opp['net_per_contract']:.4f}/contract "
                        f"contracts={opp['max_contracts']} "
                        f"expected={opp['expected_profit']:.4f}"
                    )
                    if check_risk(opp, state, cfg):
                        await execute(opp, cfg, state)
                    else:
                        logging.info(
                            f"{mode_tag} opportunity blocked by risk — pair={opp['pair_id']} "
                            f"net={opp['net_per_contract']:.4f}"
                        )

            except Exception as e:
                logging.warning(f"{mode_tag} scan error for pair {pair.get('id', '?')}: {e}")

        await asyncio.sleep(0.1)
