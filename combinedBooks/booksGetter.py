import asyncio
import logging
from datetime import datetime, timedelta
from timeit import default_timer as timer
from typing import cast

import aiohttp

from combinedBooks.exchangesData import ExchangesConstants
from combinedBooks.orderbook import OrderBookEntry, OrderBookItem
from combinedBooks.utils import nowUTCts

lgr = logging.getLogger(__name__)

CLIENT_EXCEPTIONS = (
    aiohttp.ClientResponseError,
    aiohttp.ClientConnectionError,
    aiohttp.ClientPayloadError,
    asyncio.TimeoutError,
)


class ExchangesAsyncBooksGetter:
    """Class for downloading & parsing order-books asynchronously."""

    responses: dict[str, list[dict[str, dict]]]

    def __init__(
        self,
        depth: int = 50,
        use_exchs: list[str] | None = None,
        base_pairs: list[str] | None = None,
    ) -> None:
        self.depth = depth
        self.use_exchs = use_exchs
        self.base_pairs = base_pairs
        self.xc = ExchangesConstants(self.use_exchs, self.base_pairs)
        self.exchs = self.xc.EXCH_DATA
        self.responses: dict[str, list[dict[str, dict]]] = {i: [] for i in self.exchs}

    @property
    def cls_name(self) -> str:
        return self.__class__.__name__

    async def get_single_pair(
        self,
        session: aiohttp.ClientSession,
        url: str,
        exch: str,
        pair: str,
    ) -> None:
        try:
            async with session.get(url) as response:
                resp = await response.json()
        except Exception as ex:
            print(f"{self.cls_name}.get_single_pair - Exception: {ex}")
            resp = {}
        res = {"pair": pair, "data": resp}
        self.responses[exch].append(res)

    async def _get_all_books(self) -> None:
        self.responses = {i: [] for i in self.exchs}
        async with aiohttp.ClientSession() as session:
            tasks = []
            _timer_start = timer()
            for exch, data in self.exchs.items():
                pair: str
                for pair in filter(None, data["pairs"]):
                    url = data["url"].format(pair, self.depth)
                    task = asyncio.create_task(
                        self.get_single_pair(session, url, exch, pair)
                    )
                    tasks.append(task)

            await asyncio.gather(*tasks)
            print(
                f"{self.cls_name} - Download books took: "
                f"{timedelta(seconds=timer() - _timer_start)}"
            )

    def parse_binance_obs(self) -> list[OrderBookItem]:
        """Take list of Binance orderbooks data and remove NaNs, while converting,
        string numbers to floats and creating OrderBookItems."""
        if not hasattr(self, "responses"):
            raise Exception("No responses to parse.")
        converted = []
        exch = "binance"
        for ob in self.responses[exch]:
            sym = cast(str, ob.get("pair", ""))
            data = ob.get("data")
            ts = nowUTCts() - 0.5  # assume 500ms delay
            if not data:
                continue
            obk = {
                "exchange": exch,
                "pair": self.xc.get_base_pair(exch, sym),
                "ts": ts,
                "bids": [],
                "asks": [],
                "exchs_const": self.xc,
            }
            for side in ["bids", "asks"]:
                for lvl in data[side]:
                    try:
                        obk[side].append(  # type: ignore
                            OrderBookEntry(*map(float, lvl), exch, [])  # type: ignore
                        )
                    except KeyError:
                        print(
                            f"{self.cls_name}.parse_binance_obs - KeyError: "
                            f"{sym}, {lvl}, {side}"
                        )
            converted.append(OrderBookItem(**obk))  # type: ignore
        return converted

    def parse_okx_obs(self) -> list[OrderBookItem]:
        """Take list of OKX orderbooks data and remove NaNs, while converting,
        string numbers to floats and creating OrderBookItems."""
        if not hasattr(self, "responses"):
            raise Exception("No responses to parse.")
        converted = []
        exch = "okx"
        for ob in self.responses[exch]:
            sym = cast(str, ob.get("pair", ""))
            data = ob.get("data", {}).get("data")
            if not data:
                continue
            for book in data:
                ts = int(book["ts"]) / 1000
                obk = {  # type: ignore
                    "exchange": exch,
                    "pair": sym,
                    "ts": ts,
                    "bids": [],
                    "asks": [],
                    "exchs_const": self.xc,
                }
                for side in ["bids", "asks"]:
                    for lvl in book[side]:
                        try:
                            p, s = map(float, lvl[:2])
                            obk[side].append(  # type: ignore
                                OrderBookEntry(p, s, exch, [])
                            )
                        except KeyError:
                            print(
                                f"{self.cls_name}.parse_okx_obs - KeyError: "
                                f"{sym}, {lvl}, {side}"
                            )
            converted.append(OrderBookItem(**obk))  # type: ignore
        return converted

    def parse_coinbase_obs(self) -> list[OrderBookItem]:
        """Take list of Coinbase orderbooks data and remove NaNs, while converting,
        string numbers to floats and creating OrderBookItems. Coinbase sends the
        whole book so we need to limit it to the depth we want."""
        if not hasattr(self, "responses"):
            raise Exception("No responses to parse.")
        converted = []
        exch = "coinbase"
        for ob in self.responses[exch]:
            sym = cast(str, ob.get("pair"))
            data = ob.get("data")
            if (not data) or ("message" in data) or (data["auction_mode"]):
                continue
            ts = round(datetime.fromisoformat(data["time"]).timestamp(), 3)
            obk = {
                "exchange": exch,
                "pair": self.xc.get_base_pair(exch, sym) if sym else "",
                "ts": ts,
                "bids": [],
                "asks": [],
                "exchs_const": self.xc,
            }
            for side in ["bids", "asks"]:
                for lvl in data[side][: self.depth]:
                    try:
                        p, s = map(float, lvl[:2])
                        obk[side].append(OrderBookEntry(p, s, exch, []))  # type: ignore
                    except KeyError:
                        print(
                            f"{self.cls_name}.parse_coinbase_obs - KeyError: "
                            f"{sym}, {lvl}, {side}"
                        )
            converted.append(OrderBookItem(**obk))  # type: ignore
        return converted

    async def get_all_books(self) -> dict[str, list[OrderBookItem]]:
        """Download and parse all order-books."""
        await self._get_all_books()
        obs: dict[str, list[OrderBookItem]] = {i: [] for i in self.exchs}
        for exch in self.responses:
            obs[exch] = getattr(self, f"parse_{exch}_obs")()
        return obs


async def main() -> None:
    getter = ExchangesAsyncBooksGetter()
    obs = await getter.get_all_books()
    print(obs)


if __name__ == "__main__":
    asyncio.run(main())
