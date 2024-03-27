"""
Tools for combined order books research in crypto space. Test script for combined order
books.
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
from datetime import datetime, timedelta, timezone

from combinedBooks.booksGetter import CLIENT_EXCEPTIONS, ExchangesAsyncBooksGetter
from combinedBooks.comboBooks import (
    compareComboBooks,
    multiple_join_exch_obs,
    pairs_sanity_check,
    xExchMerge,
)
from combinedBooks.utils import saveEveryNth

lgr = logging.getLogger(__name__)


async def main():
    lgr.setLevel(logging.INFO)
    lgr.addHandler(logging.StreamHandler())
    f_suffix = datetime.utcnow().strftime("%H%M%ST%d%m%y")
    results_file = f"comboResults_{f_suffix}.json"
    save_every_N_results = 200

    runForTime = timedelta(minutes=2)
    iterEverySecs = timedelta(seconds=5)
    planedIters = int(runForTime / iterEverySecs)
    print(f"Will run for {runForTime} with {iterEverySecs.seconds}sec iterations")
    print(f"Approx {planedIters} iterations")

    use_exchs = None
    base_pairs = None
    eth_amts = [50, 100, 200]
    pairs = ["ETH-USDC", "ETH-DAI", "ETH-USDT"]
    depth = 250
    debug = False
    aggLevels = True
    res = []
    bookstore = ExchangesAsyncBooksGetter(
        use_exchs=use_exchs, depth=depth, base_pairs=base_pairs
    )
    target_time = datetime.now(tz=timezone.utc) + runForTime
    while datetime.now(tz=timezone.utc) < target_time:
        try:
            obs = await bookstore.get_all_books()
            pairs_sanity_check(obs, bookstore.xc.EXCHANGES)
            toJoin = {
                "ETH-USDC": ("ETH-USDC", "ETH-DAI"),
                "BTC-USDC": ("BTC-USDC", "BTC-DAI"),
            }
            multiple_join_exch_obs(
                toJoin, obs, exchanges=bookstore.xc.EXCHANGES, aggLevels=aggLevels
            )
            merging_exch = [i for i in obs if i.endswith("_jnd")]
            merged = xExchMerge(merging_exch, obs, allCombos=True)
            for p in pairs:
                res.extend(
                    compareComboBooks(
                        p, eth_amts, False, obs | merged, toJoin, debug, aggLevels
                    )
                )
            if saveEveryNth(res, results_file, save_every_N_results):
                res = []
            planedIters -= 1
            print(f"Approx remaining iters: {planedIters}")
            await asyncio.sleep(iterEverySecs.seconds)
        except CLIENT_EXCEPTIONS as ex:
            print(ex)
            pass
        except KeyboardInterrupt:
            print("Stopped by user")
            return


if __name__ == "__main__":
    asyncio.run(main())
