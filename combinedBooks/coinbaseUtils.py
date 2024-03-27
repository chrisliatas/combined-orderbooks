"""
Tools for combined order books research in crypto space. Coinbase products data.
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

import logging
from pathlib import Path

import pandas as pd
import requests

lgr = logging.getLogger(__name__)

COINBASE_PRODUCTS = "https://api.exchange.coinbase.com/products"


class CbProducts:
    def __init__(self, data_dir: str) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.url = COINBASE_PRODUCTS
        self.products = self.get_products()
        self.products_df = self.get_products_df()
        self.products_df.to_csv(self.data_dir / "coinbaseProducts.csv", index=False)

    def setSavePairs(self) -> None:
        """Extract all unique and active (trading_disabled=False) pairs `id`."""
        _df = self.products_df.loc[~self.products_df.trading_disabled]
        _df.id.to_json(self.data_dir / "coinbasePairs.json", orient="records")
        self._pairs = _df.id.to_list()

    @property
    def pairs(self) -> list:
        """Extract all unique and active pairs by `id`."""
        try:
            return self._pairs
        except AttributeError:
            self.setSavePairs()
            return self._pairs

    def setSaveStablePairs(self) -> None:
        """Extract all unique, active (trading_disabled=False) and stable-coins
        (fx_stablecoin=True) pairs `id`."""
        _df = self.products_df.loc[
            ~self.products_df.trading_disabled & self.products_df.fx_stablecoin
        ]
        _df.id.to_json(self.data_dir / "coinbaseStablePairs.json", orient="records")
        self._stable_pairs = _df.id.to_list()

    @property
    def stable_pairs(self) -> list:
        """Extract all unique, active stable-coin pairs by `id`."""
        try:
            return self._stable_pairs
        except AttributeError:
            self.setSaveStablePairs()
            return self._stable_pairs

    def get_products(self) -> list:
        response = requests.get(self.url)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Error: {response.status_code}")

    def get_products_df(self) -> pd.DataFrame:
        try:
            df = pd.DataFrame(self.products)
        except Exception as ex:
            lgr.error(f"{self.__class__.__name__}.get_products_df Exception - {ex}")
            df = pd.DataFrame(self.products, columns=["products"])
        return df

    def get_CB_pair(self, base_pair: str) -> str:
        """Find the equivalent Coinbase Pro pair for a given base pair.
        Return USD quote if USDC is the quote currency, since in Coinbase USDC == USD"""
        if "USDC" in base_pair:
            base_pair = base_pair.replace("USDC", "USD")
        inv_pair = "-".join(base_pair.split("-")[::-1])
        try:
            cb_pair = self.products_df.loc[
                (self.products_df.id == base_pair) | (self.products_df.id == inv_pair),
                "id",
            ].iloc[0]
        except IndexError:
            lgr.warning(
                f"{self.__class__.__name__} - Pair {base_pair} not found in "
                f"Coinbase products"
            )
            cb_pair = ""
        return cb_pair
