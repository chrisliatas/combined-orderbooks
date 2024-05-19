"""
Tools for combined order books research in crypto space. Async order books getter.
    Copyright (C) 2024 Chris Liatas - cris@liatas.com

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

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
        data_dir: str,
        depth: int = 50,
        use_exchs: list[str] | None = None,
        base_pairs: list[str] | None = None,
        book_retries: int = 3,
        book_timeout: int | None = None,
        init_backoff: float = 1.0,
    ) -> None:
        self.depth = depth
        self.use_exchs = use_exchs
        self.base_pairs = base_pairs
        self.book_retries = book_retries
        self.book_timeout = book_timeout
        self.init_backoff = init_backoff
        self.xc = ExchangesConstants(data_dir, self.use_exchs, self.base_pairs)
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
        for attempt in range(self.book_retries):
            try:
                async with session.get(url, timeout=self.book_timeout) as response:
                    resp = await response.json()
                    break
            except asyncio.TimeoutError:
                lgr.warning(
                    f"{self.cls_name}.get_single_pair - Timeout, retrying "
                    f"{attempt + 1}/{self.book_retries} for {pair} on {exch}"
                )
            except Exception as ex:
                lgr.error(f"{self.cls_name}.get_single_pair - Exception: {ex}")
                resp = {}
                break
            # add an exponential backoff for each retry
            await asyncio.sleep(self.init_backoff * (2**attempt))
        else:
            lgr.error(
                f"{self.cls_name}.get_single_pair - Failed to get {pair} on {exch} "
                f"after {self.book_retries} retries."
            )
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
            lgr.info(
                f"{self.cls_name} - Download books took: "
                f"{timedelta(seconds=timer() - _timer_start)}"
            )

    def parse_binance_obs(self) -> list[OrderBookItem]:
        """Take list of Binance orderbooks data and remove NaNs, while converting,
        string numbers to floats and creating OrderBookItems."""
        if not hasattr(self, "responses"):
            raise Exception("No responses to parse.")
        converted = []
        skipped = []
        exch = "binance"
        for ob in self.responses[exch]:
            sym = cast(str, ob.get("pair", ""))
            data = ob.get("data")
            if (
                (not data)
                or (data.get("bids") in [None, []])
                or (data.get("asks") in [None, []])
            ):
                skipped.append(sym)
                continue
            ts = nowUTCts() - 0.5  # assume 500ms delay
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
                    obk[side].append(  # type: ignore
                        OrderBookEntry(*map(float, lvl), exch, [])  # type: ignore
                    )
            converted.append(OrderBookItem(**obk))  # type: ignore
        if skipped:
            lgr.warning(f"{self.cls_name}.parse_binance_obs - Skipped {skipped} pairs.")
        return converted

    def parse_okx_obs(self) -> list[OrderBookItem]:
        """Take list of OKX orderbooks data and remove NaNs, while converting,
        string numbers to floats and creating OrderBookItems."""
        if not hasattr(self, "responses"):
            raise Exception("No responses to parse.")
        converted = []
        skipped = []
        exch = "okx"
        for ob in self.responses[exch]:
            sym = cast(str, ob.get("pair", ""))
            data = ob.get("data", {}).get("data")
            if not data:
                skipped.append(sym)
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
                        p, s = map(float, lvl[:2])
                        obk[side].append(OrderBookEntry(p, s, exch, []))  # type: ignore
            converted.append(OrderBookItem(**obk))  # type: ignore
        if skipped:
            lgr.warning(f"{self.cls_name}.parse_okx_obs - Skipped {skipped} pairs.")
        return converted

    def parse_coinbase_obs(self) -> list[OrderBookItem]:
        """Take list of Coinbase orderbooks data and remove NaNs, while converting,
        string numbers to floats and creating OrderBookItems. Coinbase sends the
        whole book so we need to limit it to the depth we want."""
        if not hasattr(self, "responses"):
            raise Exception("No responses to parse.")
        converted = []
        skipped = []
        exch = "coinbase"
        for ob in self.responses[exch]:
            sym = cast(str, ob.get("pair"))
            data = ob.get("data")
            if (not data) or ("message" in data) or (data["auction_mode"]):
                skipped.append(sym)
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
                    p, s = map(float, lvl[:2])
                    obk[side].append(OrderBookEntry(p, s, exch, []))  # type: ignore
            converted.append(OrderBookItem(**obk))  # type: ignore
            if skipped:
                lgr.warning(
                    f"{self.cls_name}.parse_coinbase_obs - Skipped {skipped} pairs."
                )
        return converted

    async def get_all_books(self) -> dict[str, list[OrderBookItem]]:
        """Download and parse all order-books."""
        await self._get_all_books()
        obs: dict[str, list[OrderBookItem]] = {i: [] for i in self.exchs}
        for exch in self.responses:
            obs[exch] = getattr(self, f"parse_{exch}_obs")()
        return obs
