import asyncio
import logging

from dotenv import load_dotenv

import config
from arb import load_config, scan_loop
from feeds import kalshi_feed, load_pairs, polymarket_feed

load_dotenv()
logging.basicConfig(level=getattr(logging, config.LOG_LEVEL), format="%(asctime)s %(levelname)s %(message)s")


async def main():
    data = load_pairs("pairs.json")
    cfg = load_config("config.json")
    books = {"kalshi": {}, "poly": {}}
    state = {
        "daily_loss": 0.0,
        "total_exposure": 0.0,
        "pair_exposure": {},
        "paper_pnl": 0.0,
    }

    logging.info(f"kalshi tickers: {len(data['kalshi_tickers'])}")
    logging.info(f"poly tokens: {len(data['poly_tokens'])}")

    async with asyncio.TaskGroup() as tg:
        tg.create_task(kalshi_feed(data["kalshi_tickers"], books))
        tg.create_task(polymarket_feed(data["poly_tokens"], books))
        tg.create_task(scan_loop(data["pairs"], books, cfg, state))


if __name__ == "__main__":
    asyncio.run(main())
