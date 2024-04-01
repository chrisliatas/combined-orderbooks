"""
Tools for combined order books research in crypto space. Order book and related classes.
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

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import cached_property
from typing import Self

from combinedBooks.exchangesData import ExchangesConstants
from combinedBooks.printColors import Pcolors as ppc
from combinedBooks.utils import nowUTCts, round_digits


@dataclass
class BaseOrderBookEntry:
    price: float
    size: float
    exch: str

    def to_dict(self):
        return {"price": self.price, "size": self.size, "exch": self.exch}

    def __repr__(self):
        return f"(p={self.price}, s={self.size}, e={self.exch})"


@dataclass
class DebugOrderBookEntry(BaseOrderBookEntry):
    """Class for storing order-book entry with extra info for debug"""

    fees: float
    pair: str
    side: str

    def to_dict(self):
        book_dct = super().to_dict()
        dbg_dct = {"fees": self.fees, "pair": self.pair, "side": self.side}
        return book_dct | dbg_dct

    def __repr__(self):
        return (
            f"(p={self.price}, s={self.size}, e={self.exch}, "
            f"f={self.fees}, pair={self.pair}, side={self.side})"
        )


@dataclass
class OrderBookEntry(BaseOrderBookEntry):
    """Class for storing order-book entry with extra info for combo books."""

    debug: list[DebugOrderBookEntry]

    def _newDebugEntry(
        self, pair: str, side: str, xc: ExchangesConstants, inverse: bool = False
    ) -> DebugOrderBookEntry:
        return DebugOrderBookEntry(
            self.price,
            self.size,
            self.exch,
            xc.exchFees(self.exch, pair, inverse),
            pair,
            "BUY" if side == "asks" else "SELL",
        )

    def addDebug(
        self,
        pair: str,
        side: str,
        xc: ExchangesConstants,
        inverse: bool = False,
        erases=False,
    ) -> None:
        """Add simple debug info. Overwrites previous debug info if `erases` set to
        True."""
        _debug = [self._newDebugEntry(pair, side, xc, inverse)]
        if erases:
            self.debug = _debug
        else:
            self.debug.extend(_debug)

    def inverse(
        self,
        pair: str = "",
        side: str = "",
        deciP: int = 6,
        deciS: int = 6,
        xc: ExchangesConstants | None = None,
        debug=False,
    ) -> Self:
        """Get inverse of order-book entry. e.g. to get USDT-ETH from ETH-USDT.
        `debug` set to True will add debug info, overwriting the current debug info."""
        if debug and pair and side and xc:
            _debug = [self._newDebugEntry(pair, side, xc, True)]
        else:
            _debug = self.debug
        price = 1 / self.price
        price = round(price, round_digits(deciP, 0, price))
        size = self.size * self.price
        size = round(size, round_digits(deciS, 0, size))
        return type(self)(price, size, self.exch, _debug)

    def to_dict(self):
        book_dct = super().to_dict()
        dbg_dct = {"debug": [i.to_dict() for i in self.debug]}
        return book_dct | dbg_dct

    def __repr__(self):
        _debug = f", debug={self.debug}" if self.debug else ""
        return f"(p={self.price}, s={self.size}, e={self.exch}{_debug})"


@dataclass
class WapLevelsEntry(BaseOrderBookEntry):
    """Class for storing wap calculation levels."""

    wap: float
    amt: float

    def __repr__(self):
        return (
            f"(p={self.price}, s={self.size}, e={self.exch}, "
            f"wap={self.wap}, amt={self.amt})"
        )


@dataclass
class BookLevelIdxAmt:
    idx: int = 0
    qty: float = 0.0


@dataclass
class BookLevelsState:
    """Class for storing order-book state for wap-levels calculation."""

    asks: BookLevelIdxAmt = field(default_factory=lambda: BookLevelIdxAmt())
    bids: BookLevelIdxAmt = field(default_factory=lambda: BookLevelIdxAmt())

    def __repr__(self):
        return f"(asks={self.asks}, bids={self.bids})"


OBEntryList = list[OrderBookEntry]


class OrderBookItem:
    """Class for storing pair orderbook data per exchange.
    (Bids are sorted descending, asks ascending.)
    """

    def __init__(
        self,
        exchange: str,
        pair: str,
        ts: float,
        bids: OBEntryList,
        asks: OBEntryList,
        exchs_const: ExchangesConstants,
    ) -> None:
        self.exch = exchange
        self.pair = pair
        self.ts = ts or nowUTCts()
        self.bids = bids
        self.bids.sort(key=lambda x: x.price, reverse=True)
        self.asks = asks
        self.asks.sort(key=lambda x: x.price)
        self.xc = exchs_const
        self._quote_state = BookLevelsState()
        self._base_state = BookLevelsState()

    @property
    def date(self) -> datetime:
        return datetime.fromtimestamp(self.ts, tz=timezone.utc)

    @property
    def bidsLen(self) -> int:
        return len(self.bids)

    @property
    def asksLen(self) -> int:
        return len(self.asks)

    @property
    def bidsTotSize(self) -> float:
        tot = 0.0
        for i in self.bids:
            tot += i.size
        return tot

    @property
    def asksTotSize(self) -> float:
        tot = 0.0
        for i in self.asks:
            tot += i.size
        return tot

    @property
    def spread(self) -> float:
        return self.asks[0].price - self.bids[0].price

    @property
    def mid(self) -> float:
        return (self.asks[0].price + self.bids[0].price) / 2

    def _getDecimals(self, side: str, isPrice: bool = True) -> int:
        deci = 1
        for i in getattr(self, side):
            f_var = i.price if isPrice else i.size
            try:
                deci = max(deci, len(str(f_var).split(".")[1]))
            except IndexError:
                # probably scientific notation, e.g. 1e-8
                deci = max(deci, int(str(f_var).split("-")[1]))
        return deci

    @cached_property
    def lenPrcDecimals(self) -> int:
        deci = self._getDecimals("bids")
        return max(deci, self._getDecimals("asks"))

    @cached_property
    def lenSizeDecimals(self) -> int:
        deci = self._getDecimals("bids", False)
        return max(deci, self._getDecimals("asks", False))

    def roundPriceToDecimal(self, dec: int = 6) -> None:
        """Round prices to given decimal."""
        for i in self.bids:
            i.price = round(i.price, dec)
        for i in self.asks:
            i.price = round(i.price, dec)

    def sideAfterFees(
        self, side: str, add_fee: float = 0.0, inverse: bool = False
    ) -> OBEntryList:
        """Return side after fees. We calculate fees as TAKER sees them."""
        entries = []
        _sign = -1 if side == "bids" else 1
        for i in getattr(self, side):
            new_fee = self.xc.exchFees(i.exch, self.pair, inverse) + add_fee
            prc = round(i.price * (1 + _sign * new_fee), self.lenPrcDecimals)
            _debug = i.debug
            if add_fee:
                _debug = i.debug.copy()
                _debug.append(
                    DebugOrderBookEntry(
                        i.price,
                        i.size,
                        i.exch,
                        new_fee,
                        self.pair,
                        "BUY" if side == "asks" else "SELL",
                    )
                )
            entries.append(OrderBookEntry(prc, i.size, i.exch, _debug))
        return entries

    def bidsAfterFees(self, add_fee: float = 0.0, inverse: bool = False) -> OBEntryList:
        return self.sideAfterFees("bids", add_fee, inverse)

    def asksAfterFees(self, add_fee: float = 0.0, inverse: bool = False) -> OBEntryList:
        return self.sideAfterFees("asks", add_fee, inverse)

    def wap_base(self, base_qty: float, side="asks", incl_fees=False) -> float:
        """Return weighted average price for given base quantity."""
        levels = self.asksAfterFees() if incl_fees else self.asks
        if side == "bids":
            levels = self.bidsAfterFees() if incl_fees else self.bids
        tot = 0.0
        qty = base_qty
        for i in levels:
            if qty >= i.size:
                tot += i.price * i.size
                qty -= i.size
            else:
                tot += i.price * qty
                break
        return tot / base_qty

    def wap_quote(self, quote_qty: float, side="asks", incl_fees=False) -> float:
        """Return weighted average price for given quote quantity."""
        levels = self.asksAfterFees() if incl_fees else self.asks
        if side == "bids":
            levels = self.bidsAfterFees() if incl_fees else self.bids
        tot_size = 0.0
        qty = quote_qty
        for i in levels:
            if qty >= (amt := i.size * i.price):
                tot_size += i.size
                qty -= amt
            else:
                tot_size += qty / i.price
                break
        return quote_qty / tot_size if tot_size else 0.0

    def wap_base_levels(
        self, base_qty: float, side="asks", incl_fees=False
    ) -> list[WapLevelsEntry]:
        """Returns a list of WapLevelsEntry for given base quantity, with all the levels
        that were used to reach the given base quantity. It keeps track of current
        order-book state, so that the same book can be used to calculate multiple WAPs.
        """
        levels = self.asksAfterFees() if incl_fees else self.asks
        if side == "bids":
            levels = self.bidsAfterFees() if incl_fees else self.bids
        qty = base_qty
        res = []
        state = getattr(self._base_state, side)
        ignore_size = state.qty
        start_idx = state.idx
        for lvl in levels[start_idx:]:
            lvl_base_qty = lvl.size - ignore_size
            lvl_quote_amt = lvl_base_qty * lvl.price
            if qty >= lvl_base_qty:
                res.append(
                    WapLevelsEntry(
                        lvl.price,
                        lvl_base_qty,
                        lvl.exch,
                        lvl.price,
                        lvl_quote_amt,
                    )
                )
                qty -= lvl_base_qty
                ignore_size = 0.0
                state.idx += 1
                state.qty = 0.0
            else:
                lvl_quote_amt = qty * lvl.price
                res.append(
                    WapLevelsEntry(lvl.price, qty, lvl.exch, lvl.price, lvl_quote_amt)
                )
                ignore_size += qty
                state.qty = ignore_size
                break

        return res

    def wap_quote_levels(
        self, quote_qty: float, side="asks", incl_fees=False
    ) -> list[WapLevelsEntry]:
        """Returns a list of WapLevelsEntry for given quote quantity, with all the
        levels that were used to reach the given quote quantity. It keeps track of
        current order-book state, so that the same book can be used to calculate
        multiple WAPs.
        """
        levels = self.asksAfterFees() if incl_fees else self.asks
        if side == "bids":
            levels = self.bidsAfterFees() if incl_fees else self.bids
        qty = quote_qty
        res = []
        state = getattr(self._quote_state, side)
        ignore_size = state.qty
        start_idx = state.idx
        for lvl in levels[start_idx:]:
            lvl_base_qty = lvl.size - ignore_size
            lvl_quote_amt = lvl_base_qty * lvl.price
            if qty >= lvl_quote_amt:
                res.append(
                    WapLevelsEntry(
                        lvl.price,
                        lvl_base_qty,
                        lvl.exch,
                        lvl.price,
                        lvl_quote_amt,
                    )
                )
                qty -= lvl_quote_amt
                ignore_size = 0.0
                state.idx += 1
                state.qty = 0.0
            else:
                res.append(
                    WapLevelsEntry(lvl.price, qty / lvl.price, lvl.exch, lvl.price, qty)
                )
                ignore_size += qty / lvl.price
                state.qty = ignore_size
                break

        return res

    def reset_wap_state(self) -> None:
        """Reset wap state."""
        self._quote_state = BookLevelsState()
        self._base_state = BookLevelsState()

    def addSideDebug(self, pair: str, side: str, erases=False) -> None:
        """Add debug info to all levels of given side. Overwrites previous debug info if
        `erases` set to True."""
        for i in getattr(self, side):
            i.addDebug(pair, side, erases)

    def addLevelsDebug(self, pair: str, erases=False) -> None:
        """Add debug info to all levels. Overwrites previous debug info if `erases` set
        to True."""
        self.addSideDebug(pair, "bids", erases)
        self.addSideDebug(pair, "asks", erases)

    def aggregateSideLevels(self, side: str, debug=False) -> OBEntryList:
        """Aggregate levels with same price."""
        levels = getattr(self, side)
        res: OBEntryList = []
        for i in levels:
            if not res or res[-1].price != i.price:
                res.append(OrderBookEntry(i.price, i.size, i.exch, i.debug))
            else:
                res[-1].size += i.size
                res[-1].size = round(
                    res[-1].size,
                    round_digits(
                        self.lenSizeDecimals, self.lenSizeDecimals, res[-1].size
                    ),
                )
                if debug:
                    res[-1].debug.extend(i.debug)
        return res

    def aggregateLevels(self, debug=False) -> None:
        """Aggregate levels with same price."""
        self.bids = self.aggregateSideLevels("bids", debug)
        self.asks = self.aggregateSideLevels("asks", debug)

    def inverseBook(self, debug=False) -> Self:
        """Return inverse order-book."""
        pair = "-".join(self.pair.split("-")[::-1])
        bids = [
            i.inverse(
                self.pair,
                "bids",
                self.lenSizeDecimals,
                self.lenPrcDecimals,
                self.xc,
                debug,
            )
            for i in self.bids
        ]
        asks = [
            i.inverse(
                self.pair,
                "asks",
                self.lenSizeDecimals,
                self.lenPrcDecimals,
                self.xc,
                debug,
            )
            for i in self.asks
        ]
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)
        return type(self)(self.exch, pair, self.ts, bids, asks, self.xc)

    def __repr__(self):
        return (
            f"\n<{ppc.CBOLD}{self.exch}{ppc.CEND}, {self.pair}, {self.date}, "
            f"Levels asks: {self.asksLen}, bids: {self.bidsLen}, Size asks: "
            f"{round(self.asksTotSize, 6)}, bids: {round(self.bidsTotSize, 6)}\n"
            f"{ppc.CBOLD}{ppc.CRED}Asks:{ppc.CEND} {self.asks}\n"
            f"{ppc.CBOLD}{ppc.CGREEN}Bids:{ppc.CEND} {self.bids}>"
        )

    def to_dict(self):
        """Return JSON serializable dict of OrderBookItem."""
        return {
            "exch": self.exch,
            "pair": self.pair,
            "ts": self.ts,
            "bids": [i.to_dict() for i in self.bids],
            "asks": [i.to_dict() for i in self.asks],
            "date": self.date.isoformat(timespec="milliseconds"),
        }

    def copy_self(self) -> Self:
        """Return safe copy of OrderBookItem."""
        return type(self)(
            self.exch,
            self.pair,
            self.ts,
            deepcopy(self.bids),
            deepcopy(self.asks),
            self.xc,
        )
