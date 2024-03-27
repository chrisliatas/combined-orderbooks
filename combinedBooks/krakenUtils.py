"""
Tools for combined order books research in crypto space. Kraken products data.
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

import pandas as pd
import requests

from combinedBooks.utils import DATA_DIR

KRAKEN_PRODUCTS = "https://api.kraken.com/0/public/AssetPairs"

# https://support.kraken.com/hc/en-us/articles/360001185506-How-to-interpret-asset-codes


class KrakenProducts:
    def __init__(self) -> None:
        self.url = KRAKEN_PRODUCTS
        self.products = self.get_products()
        self.products_df = self.get_products_df()
        self.products_df.to_csv(DATA_DIR / "krakenProducts.csv", index=False)

    def setSavePairs(self) -> None:
        """Extract all unique and active ("status": "online") pairs keys."""
        _df = self.products_df.loc[self.products_df.status == "online"]
        _df.pair.to_json(DATA_DIR / "krakenPairs.json", orient="records")
        self._pairs = _df.pair.to_list()

    @property
    def pairs(self) -> list:
        """Extract all unique and active pairs keys."""
        try:
            return self._pairs
        except AttributeError:
            self.setSavePairs()
            return self._pairs

    def get_products(self) -> dict:
        response = requests.get(self.url)
        if response.status_code == 200:
            return response.json()["result"]
        else:
            print(f"Error {response.status_code}")
            return {}

    def get_products_df(self) -> pd.DataFrame:
        """Convert products dict to DataFrame."""
        try:
            df = pd.DataFrame.from_dict(self.products, orient="index")
            df.index.name = "pair"
            df.reset_index(inplace=True)
        except Exception as ex:
            print(ex)
            df = pd.DataFrame(self.products, columns=["products"])
        return df

    def get_KKN_pair(self, base_pair: str) -> str:
        """Find the equivalent Kraken pair for the given base pair."""
        if "BTC" in base_pair:
            base_pair = base_pair.replace("BTC", "XBT")
        inv_pair = "".join(base_pair.split("-")[::-1])
        base_pair = base_pair.replace("-", "")
        try:
            kk_pair = self.products_df.loc[
                (self.products_df.altname == base_pair)
                | (self.products_df.altname == inv_pair),
                "altname",
            ].iloc[0]
        except IndexError:
            print(f"Pair {base_pair} not found in Kraken products")
            kk_pair = ""
        return kk_pair


def main() -> None:
    kp = KrakenProducts()
    print(kp.products)


if __name__ == "__main__":
    main()
