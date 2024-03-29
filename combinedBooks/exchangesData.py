"""
Tools for combined order books research in crypto space. Excahnge data constants.
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


Constants for exchanges data.

Fee tiers reference:
https://www.binance.com/en/fee/trading
https://www.okx.com/fees
https://help.coinbase.com/en/pro/trading-and-funding/trading-rules-and-fees/fees
https://www.coinbase.com/advanced-fees
https://www.kraken.com/features/fee-schedule

"""

from combinedBooks.coinbaseUtils import CbProducts


class ExchangesConstants:
    avail_exchanges = ["binance", "okx", "coinbase"]
    EXCH_FEES = {
        "binance": {"spot": 0.000405, "stables": 0.0},
        "okx": 0.0004,
        "coinbase": {"spot": 0.001, "stables": 0.00001},
    }
    BASE_PAIRS = [
        "ETH-USDC",
        "USDC-USDT",
        "BTC-USDC",
        "ETH-BTC",
        "ETH-USDT",
        "ETH-DAI",
        "BTC-DAI",
    ]
    VALID_QUOTES = ["DAI", "USDT", "BUSD", "USDC", "BTC", "WBTC", "WETH", "ETH"]
    BINANCE_BOOKS = "https://api.binance.com/api/v3/depth?symbol={}&limit={}"
    COINBASE_BOOKS = "https://api.exchange.coinbase.com/products/{}/book?level=2"
    OKX_BOOKS = "https://www.okx.com/api/v5/market/books?instId={}&sz={}"

    def __init__(
        self,
        data_dir: str,
        use_exchs: list[str] | None = None,
        base_pairs: list[str] | None = None,
    ) -> None:
        self.use_exchs = use_exchs
        if self.use_exchs:
            self.EXCHANGES = [
                exch for exch in self.avail_exchanges if exch in self.use_exchs
            ]
        else:
            self.EXCHANGES = self.avail_exchanges
        print(f"{self.__class__.__name__} - Using exchanges: {self.EXCHANGES}")
        self.base_pairs = base_pairs
        if self.base_pairs:
            self.BASE_PAIRS = self.base_pairs
        if "coinbase" in self.EXCHANGES:
            self.cbProds = CbProducts(data_dir)
            self.COINBASE_PAIRS = list(map(self.cbProds.get_CB_pair, self.BASE_PAIRS))
        if "binance" in self.EXCHANGES:
            self.BINANCE_PAIRS = [i.replace("-", "") for i in self.BASE_PAIRS]
            self.BINANCE_STABLES = ["USDCUSDT"]
        if "okx" in self.EXCHANGES:
            self.OKX_PAIRS = self.BASE_PAIRS
        self.EXCH_DATA = {
            i: {
                "url": getattr(self, f"{i.upper()}_BOOKS"),
                "pairs": getattr(self, f"{i.upper()}_PAIRS"),
                "fees": self.EXCH_FEES[i],
                "pairs_key": {
                    k: v
                    for k, v in zip(
                        getattr(self, f"{i.upper()}_PAIRS"), self.BASE_PAIRS
                    )
                },
            }
            for i in self.EXCHANGES
        }

    def get_base_pair(self, exch: str, pair: str) -> str:
        """Return base pair per exchange for given pair."""
        return self.EXCH_DATA[exch]["pairs_key"][pair]

    def get_exch_pair(self, exch: str, pair: str) -> str:
        """Return exchange pair for given base pair. Opposite of `get_base_pair`, so
        we get key from value."""
        return list(self.EXCH_DATA[exch]["pairs_key"].keys())[
            list(self.EXCH_DATA[exch]["pairs_key"].values()).index(pair)
        ]

    def exchFees(self, exch: str, pair: str = "", inverse: bool = False) -> float:
        """Return exchange fees. Account for `_joined` exchanges.
        Coinbase Pro fees are split between `spot` and `stables`, so we need to
        account for that as well, using `pair` to determine which fee to return."""
        if inverse:
            pair = "-".join(pair.split("-")[::-1])
        if "_joined" in exch:
            exch = exch.split("_")[0]
        print(f"exch: {exch}, pair: {pair}, is inverse: {inverse}")
        if exch == "coinbase":
            if pair:
                # check if pair in Coinbase `stables`.
                if self.get_exch_pair(exch, pair) in self.cbProds.stable_pairs:
                    return self.EXCH_DATA[exch]["fees"]["stables"]
            return self.EXCH_DATA[exch]["fees"]["spot"]
        if exch == "binance":
            if pair:
                # check if pair in Binance `stables`.
                if self.get_exch_pair(exch, pair) in self.BINANCE_STABLES:
                    return self.EXCH_DATA[exch]["fees"]["stables"]
            return self.EXCH_DATA[exch]["fees"]["spot"]

        return self.EXCH_DATA[exch]["fees"]

    def comboFees(self, exch_pair: list[tuple[str, str]]) -> float:
        """Return combo fees for given exchanges."""
        return sum([self.exchFees(exch, pair) for exch, pair in exch_pair])
