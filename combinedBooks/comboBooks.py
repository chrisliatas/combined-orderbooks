"""
Tools for combined order books research in crypto space. Combine order books logic.
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
from dataclasses import dataclass
from itertools import chain, combinations

from combinedBooks.orderbook import DebugOrderBookEntry, OrderBookEntry, OrderBookItem
from combinedBooks.utils import round_digits

lgr = logging.getLogger(__name__)


def find_pairs(
    wanted_pair: str, known_pairs: list[str], valid_quotes: list[str]
) -> list[tuple[str, str]]:
    """Synthesize a pair from a list of known pairs. The pair is synthesized by finding
    two pairs that share a common quote currency, so that the quote currency can be
    canceled out. The quote currency must be in the `valid_quotes` list.
    Args:
        wanted_pair: The pair you want to synthesize.
        known_pairs: The list of known pairs.
    Returns:
        A list of tuples of two base pairs to create the synthetic from.
    Example:
        wanted_pair = "KNC-ETH",
        known_pairs = ["ETH-USDT", "USDC-USDT", "KNC-USDT", "ETH-DAI"]
        Returns: [("KNC-USDT", "ETH-USDT")]
    """
    # check if wanted_pair is in known_pairs
    base, quote = wanted_pair.split("-")
    if existing_pairs := [p for p in known_pairs if quote in p and base in p]:
        return [(existing_pairs[0], existing_pairs[0])]
    # Find all pairs that share the same base currency
    common_base_pairs = [
        p for p in known_pairs if p.startswith(base) or p.endswith(base)
    ]
    # lgr.debug(f"find_pairs - common_base_pairs: {common_base_pairs}")
    # Extract the quote currency from each pair & remove '-' from the quote.
    common_quotes = list(set([p.split("-")[1] for p in common_base_pairs]))
    # Remove quotes that are not in the valid_quotes list
    common_quotes = [quote for quote in common_quotes if quote in valid_quotes]
    # Find all pairs that share the same common_quotes currencies and include `quote`
    # lgr.debug(f"find_pairs - common_quotes: {common_quotes}")
    related_quote_pairs = []
    idx = 1
    for q in common_quotes:
        related_quote_pairs += [p for p in known_pairs if (q in p) and (quote in p)]
    if not related_quote_pairs:
        # let's try to find a pair with related base currency from common_base_pairs.
        for pair in common_base_pairs:
            related_quote_pairs += [
                p for p in known_pairs if (pair.split("-")[0] in p) and (quote in p)
            ]
        idx = 0
    # lgr.debug(f"find_pairs - related_quote_pairs: {related_quote_pairs}")
    # filter `common_base_pairs` excluding pairs with quote not in `related_quote_pairs`
    if related_quote_pairs:
        common_base_pairs = [
            p
            for p in common_base_pairs
            if p.split("-")[idx] in ",".join(related_quote_pairs)
        ]
        # lgr.debug(f"find_pairs - common_base_pairs: {common_base_pairs}")
    # Match the pairs with common quotes to create list of tuples
    return [i for i in zip(common_base_pairs, related_quote_pairs)]


def get_exch_book(
    exch: str,
    pair: str,
    obs: dict[str, list[OrderBookItem]],
    as_copy: bool = True,
    fallback: bool = False,
) -> OrderBookItem | None:
    """Get order-book from list of order-books by pair."""
    if exch_books := obs.get(exch):
        for ob in exch_books:
            if ob.pair == pair:
                return ob.copy_self() if as_copy else ob
    if fallback and exch.endswith("_jnd"):
        # try to find pair in original exchange
        _exc = exch.split("_")[0]
        print(f"get_exch_book - Falling back to {_exc} to get {pair}")
        return get_exch_book(_exc, pair, obs, as_copy)
    print(f"get_exch_book - Could not find {pair} for {exch}")
    return None


def nBooksJoin(
    obL: list[OrderBookItem],
    pair: str = "",
    exch: str = "",
    add_fees: bool = False,
    aggLevels=False,
) -> OrderBookItem:
    """Join N order-books. The function assumes compatible pairs.
    Args:
        obL: List of order-books to join.
        pair: The name for the output (joined) pair. Defaults to obL[0].pair.
        exch: The output exchange name to use. Defaults to obL[0].exch.
        add_fees: Whether to add fees to the joined pair order-book."""
    _pair = pair or obL[0].pair
    _exch = exch or obL[0].exch
    _xc = obL[0].xc
    ts = max([i.ts for i in obL])
    if add_fees:
        bids = list(chain.from_iterable([i.bidsAfterFees() for i in obL]))
        asks = list(chain.from_iterable([i.asksAfterFees() for i in obL]))
    else:
        bids = list(chain.from_iterable([i.bids for i in obL]))
        asks = list(chain.from_iterable([i.asks for i in obL]))
    bids.sort(key=lambda x: x.price, reverse=True)
    asks.sort(key=lambda x: x.price)
    new_book = OrderBookItem(_exch, _pair, ts, bids, asks, _xc)
    if aggLevels:
        new_book.aggregateLevels()
    return new_book


def join_exch_obs(
    exch: str,
    inp1: str,
    inp2: str,
    joined_pair: str,
    obs: dict[str, list[OrderBookItem]],
    add_fees: bool = False,
) -> OrderBookItem | None:
    """Join order-books from the same exchange. The function assumes compatible pairs,
    to return the joined order-book. Bids and asks are simply appended, and then sorted.
    Example case: DAI may be considered as 1:1 with USDC, so pairs ETH-USDC and ETH-DAI
    can be joined.
    Args:
        exch: The exchange to use.
        inp1: The first pair to join.
        inp2: The second pair to join.
        joined_pair: The name for the output (joined) pair.
        obs: The common order-book dictionary for all exchanges.
        add_fees: Whether to add fees to the joined pair order-book."""
    ob1 = get_exch_book(exch, inp1, obs)
    ob2 = get_exch_book(exch, inp2, obs)
    if not ob1 or not ob2:
        return None
    return nBooksJoin([ob1, ob2], joined_pair, exch, add_fees)


def multiple_join_exch_obs(
    toJoin: dict[str, tuple[str, str]],
    obs: dict[str, list[OrderBookItem]],
    exchanges: list[str],
    keep_both: bool = False,
    add_fees: bool = False,
    aggLevels=False,
) -> None:
    """Join multiple pairs of order-books from the same exchange. This will produce new
    keys in the `obs` dictionary, with the name of the exchange and the suffix '_jnd'.
    Args:
        toJoin: Dictionary of {joined_pair : tuples of pairs to merge}.
        obs: The common order-book dictionary for all exchanges.
        exchanges: The exchanges to merge.
        keep_both: Whether to keep both pairs after merging.
        add_fees: Whether to add fees to the merged pair order-book."""
    pairs_required = set(chain.from_iterable(toJoin.values()))
    for exch in exchanges:
        join = f"{exch}_jnd"
        obs[join] = [i.copy_self() for i in obs[exch] if i.pair in pairs_required]
        if not obs[join]:
            print(f"multiple_join_exch_obs - No pairs to join for {exch}")
            continue
        for joined_pair, (inp1, inp2) in toJoin.items():
            if new_book := join_exch_obs(join, inp1, inp2, joined_pair, obs, add_fees):
                if keep_both:
                    new_book.exch = join
                    if aggLevels:
                        new_book.aggregateLevels()
                    obs[join].append(new_book)
                else:
                    # replace inp1 with new_book and remove inp2.
                    if ob1 := get_exch_book(join, inp1, obs, as_copy=False):
                        ob1.pair = joined_pair
                        ob1.exch = join
                        ob1.bids = new_book.bids
                        ob1.asks = new_book.asks
                        if aggLevels:
                            ob1.aggregateLevels()
                    if ob2 := get_exch_book(join, inp2, obs, as_copy=False):
                        obs[join].remove(ob2)
            else:
                print(
                    f"multiple_join_exch_obs - Could not merge {inp1} and {inp2} "
                    f"for {exch}"
                )


def get_exch_obs_pairs(exch: str, obs: dict[str, list[OrderBookItem]]) -> list[str]:
    """Return list of pairs in order-books for given exchange."""
    return list(set(i.pair for i in obs[exch]))


def xExchMerge(
    exchanges: list[str],
    obs: dict[str, list[OrderBookItem]],
    allCombos: bool = False,
    aggLevels=False,
) -> dict[str, list[OrderBookItem]]:
    """Merge order-books cross exchanges on common pairs. `obs` may contain books that
    have been joined, so, unique exchanges must be selected otherwise duplicates will be
    introduced. Books are merged after applying fees per level.
    Args:
        exchanges: The exchanges to merge.
        obs: The common order-book dictionary for all exchanges.
        allCombos: Whether to merge all possible combinations of exchanges. False will
            only merge the superset.
        aggLevels: Whether to aggregate same price levels."""
    # find common pairs
    pairs = {i: get_exch_obs_pairs(i, obs) for i in exchanges}
    common_pairs = list(set.intersection(*map(set, pairs.values())))  # type: ignore
    # get books
    exch_books = {
        i: [j.copy_self() for j in obs[i] if j.pair in common_pairs] for i in exchanges
    }
    # sort books by pair
    for i in exch_books.values():
        i.sort(key=lambda x: x.pair)
    # merge books with all possible combinations
    res = {}
    merged: list[OrderBookItem] = []
    if allCombos:
        for n in range(2, len(exchanges) + 1):
            for j in combinations(exchanges, n):
                _exch = "-".join(j)
                for k in zip(*[exch_books[i] for i in j]):
                    merged.append(nBooksJoin(list(k), k[0].pair, _exch, True))
                    if aggLevels:
                        merged[-1].aggregateLevels()
                res[_exch] = merged
                merged = []
    else:
        _exch = "-".join(exchanges)
        for i in zip(*exch_books.values()):  # type: ignore
            merged.append(nBooksJoin(list(i), i[0].pair, _exch, True))
            if aggLevels:
                merged[-1].aggregateLevels()
        res[_exch] = merged
    return res


@dataclass
class CombineCaseLogic:
    name: str
    pair: str
    p1: str
    p2: str
    asks: tuple[str, str]
    bids: tuple[str, str]


def get_cases(pair: str, p1: str, p2: str) -> dict[str, CombineCaseLogic]:
    """Create the logic for combining the pairs. (See `case_select` for details.)"""
    return {
        "common_quote": CombineCaseLogic(
            "common_quote", pair, p1, p2, ("asks", "bids"), ("bids", "asks")
        ),
        "common_base": CombineCaseLogic(
            "common_base", pair, p1, p2, ("bids", "asks"), ("asks", "bids")
        ),
        "quote_base": CombineCaseLogic(
            "quote_base", pair, p1, p2, ("asks", "asks"), ("bids", "bids")
        ),
        "base_quote": CombineCaseLogic(
            "base_quote", pair, p1, p2, ("bids", "bids"), ("asks", "asks")
        ),
    }


def case_select(pair: str, p1: str, p2: str) -> CombineCaseLogic | None:
    """Given the component pairs produced by find_pairs, return the correct case to
    combine the pairs.
    Case 1: Assume KNC-ETH is the wanted pair and [('KNC-USDT', 'ETH-USDT'),
    ('KNC-BTC', 'ETH-BTC')] is the result of find_pairs (common currency is quote
    in tuple) pair1 from find_pairs has the same base (KNC) as pair and we know
    ETH is the output quote. We want asks from pair1 and bids from pair2, to
    create KNC-ETH asks and the opposite for bids. If ETH-KNC is the output pair,
    find_pairs will reverse the order of pairs within the tuple, so we can use the
    same logic.
    Case 2: Assume DAI-USDT wanted and [('ETH-DAI', 'ETH-USDT'),
    ('BTC-DAI', 'BTC-USDT')] is the result of find_pairs (common currency is base
    in tuple). We want bids from pair1 and asks from pair2, to create DAI-USDT
    asks and the opposite for bids. If USDT-DAI is the output pair, find_pairs
    will reverse the order of pairs within the tuple, so we can use the same
    logic.
    Case 3: Assume KNC-DAI wanted and [('KNC-BTC', 'BTC-DAI')] output of
    find_pairs (quote of first pair is base of the second). We want asks from
    pair1 and asks from pair2, to create KNC-DAI asks and the opposite for bids.
    Case 4: Assume DAI-KNC wanted and [('BTC-DAI', 'KNC-BTC')] output of
    find_pairs (base of the first pair is quote of the second). We want bids from
    pair1 and bids from pair2, to create DAI-KNC asks and the opposite for bids.
    """
    cases = get_cases(pair, p1, p2)
    b1, q1 = p1.split("-")
    b2, q2 = p2.split("-")
    if q1 == q2:
        return cases["common_quote"]
    elif b1 == b2:
        return cases["common_base"]
    elif q1 == b2:
        return cases["quote_base"]
    elif b1 == q2:
        return cases["base_quote"]
    else:
        return None


def convert_side_quote(
    ob1: OrderBookItem,
    ob2: OrderBookItem,
    side1: str,
    side2: str,
    debug: bool = False,
) -> list[OrderBookEntry]:
    """Rebalance quote/base to final quote/base price."""
    entries = []
    sideA1 = getattr(ob1, side1)
    # for each lvl in sideA1, get quote amount (price * size) and convert to base
    # using ob2 (base) wap_quote to produce new ob levels for asks
    for i in sideA1:
        levels = ob2.wap_quote_levels(i.price * i.size, side=side2)
        for lvl in levels:
            prc = i.price / lvl.wap
            size = lvl.size / prc
            size = round(
                size, round_digits(ob1.lenSizeDecimals, ob2.lenSizeDecimals, size)
            )
            fees = ob1.xc.comboFees([(i.exch, ob1.pair), (lvl.exch, ob2.pair)])
            prcWfees = prc * (1 + fees)
            prcWfees = round(
                prcWfees, round_digits(ob1.lenPrcDecimals, ob2.lenPrcDecimals, prcWfees)
            )
            _debug: list[DebugOrderBookEntry] = []
            if debug:
                _debug = [
                    DebugOrderBookEntry(
                        i.price,
                        size,
                        i.exch,
                        ob1.xc.exchFees(i.exch, ob1.pair),
                        ob1.pair,
                        "BUY" if side1 == "asks" else "SELL",  # taker buys/sells
                    ),
                    DebugOrderBookEntry(
                        lvl.price,
                        lvl.size,
                        lvl.exch,
                        ob2.xc.exchFees(lvl.exch, ob2.pair),
                        ob2.pair,
                        "BUY" if side2 == "asks" else "SELL",  # taker buys/sells
                    ),
                ]
            entries.append(OrderBookEntry(prcWfees, size, "merged", _debug))
    return entries


def convert_side_base(
    ob1: OrderBookItem,
    ob2: OrderBookItem,
    side1: str,
    side2: str,
    debug: bool = False,
) -> list[OrderBookEntry]:
    """Rebalance quote/base to final quote/base price."""
    entries = []
    sideA1 = getattr(ob1, side1)
    # for each lvl in sideA1, get quote amount (price * size) and convert to base
    # using ob2 (base) wap_quote to produce new ob levels for asks
    for i in sideA1:
        levels = ob2.wap_base_levels(i.price * i.size, side=side2)
        for lvl in levels:
            prc = i.price * lvl.wap
            size = round(
                lvl.size,
                round_digits(ob1.lenSizeDecimals, ob2.lenSizeDecimals, lvl.size),
            )
            fees = ob1.xc.comboFees([(i.exch, ob1.pair), (lvl.exch, ob2.pair)])
            prcWfees = prc * (1 + fees)
            prcWfees = round(
                prcWfees, round_digits(ob1.lenPrcDecimals, ob2.lenPrcDecimals, prcWfees)
            )
            _debug: list[DebugOrderBookEntry] = []
            if debug:
                _debug = [
                    DebugOrderBookEntry(
                        i.price,
                        i.size,
                        i.exch,
                        ob1.xc.exchFees(i.exch, ob1.pair),
                        ob1.pair,
                        "BUY" if side1 == "asks" else "SELL",  # taker buys/sells
                    ),
                    DebugOrderBookEntry(
                        lvl.price,
                        lvl.size,
                        lvl.exch,
                        ob2.xc.exchFees(lvl.exch, ob2.pair),
                        ob2.pair,
                        "BUY" if side2 == "asks" else "SELL",  # taker buys/sells
                    ),
                ]
            entries.append(OrderBookEntry(prcWfees, size, "merged", _debug))
    ob2.reset_wap_state()
    return entries


def matchFromJoined(pair: str, joinedPs: dict[str, tuple[str, str]]) -> str:
    """Return the pair that has been joined with the given pair."""
    for k, v in joinedPs.items():
        if pair in v:
            return k
    return pair


def combo_by_conversion(
    pair: str,
    exch: str,
    obs: dict[str, list[OrderBookItem]],
    known_pairs: list[str] | None = None,
    debug: bool = False,
    aggLevels=False,
) -> list[OrderBookItem] | None:
    """Return combined order-book for given pair. This function will create a synthetic
    pair for the given pair by converting the quote/base currency to the final
    quote/base using known pairs. The same logic will be applied to any given pair, even
    if it is a known pair. eg. ETH-USDC combo-book will be produced combining ETH-USDT
    and USDT-USDC, to provide a synthetic ETH-USDC pair, even though ETH-USDC is a known
    pair.
    The process is as follows:
    1. Synthesize the wanted pair from known (component) pairs.
    2. Find the case, depending on the implied orders, to combine the component pairs.
    Check `case_select` function for details
    Args:
        pair: The pair to return the order-book for.
        exch: The exchange order-book to use for creating the combo book.
        obs: The common order-book dictionary for all exchanges.
        known_pairs: The list of known pairs.
        debug: Whether to include debug info in the return book.
        aggLevels: Whether to aggregate same price levels.
    """
    if not known_pairs:
        known_pairs = get_exch_obs_pairs(exch, obs)
    _ob = obs[next(iter(obs))][0]
    comp_pairs = find_pairs(pair, known_pairs, _ob.xc.VALID_QUOTES)
    print(f"combo_by_conversion - comp_pairs: {comp_pairs}")
    books: list[OrderBookItem] = []
    for p1, p2 in comp_pairs:
        asks: list[OrderBookEntry] = []
        bids: list[OrderBookEntry] = []
        ob1 = get_exch_book(exch, p1, obs, fallback=True)
        ob2 = get_exch_book(exch, p2, obs, fallback=True)
        if not ob1 or not ob2:
            continue
        if case := case_select(pair, p1, p2):
            print(f"combo_by_conversion - Using case `{case.name}`")
            if case.name == "common_quote":
                asks = convert_side_quote(ob1, ob2, *case.asks, debug)
                bids = convert_side_quote(ob1, ob2, *case.bids, debug)
            elif case.name == "common_base":
                asks = convert_side_base(ob1, ob2, *case.asks, debug)
                bids = convert_side_base(ob1, ob2, *case.bids, debug)
            elif case.name == "quote_base":
                asks = convert_side_quote(ob1, ob2, *case.asks, debug)
                bids = convert_side_quote(ob1, ob2, *case.bids, debug)
            else:
                # case.name == "base_quote"
                asks = convert_side_base(ob1, ob2, *case.asks, debug)
                bids = convert_side_base(ob1, ob2, *case.bids, debug)
        else:
            print(f"combo_by_conversion - No case for {p1} and {p2}")
        if asks and bids:
            new_bk = OrderBookItem(exch, pair, ob1.ts, bids, asks, ob1.xc)
            if aggLevels:
                new_bk.aggregateLevels(debug)
            books.append(new_bk)
    return books if books else None


def get_taker_book(
    final_pair: str,
    pair: str,
    exch: str,
    obs: dict[str, list[OrderBookItem]],
    inverse=False,
    debug=False,
    aggLevels=False,
) -> OrderBookItem | None:
    """Return an order-book for the given pair and exchange, including fees, as seen
    by the taker.
    Args:
        final_pair: The pair to return the order-book for.
        pair: The pair to search in the exchange books for.
        exch: The exchange order-book to use for creating the combo book.
        obs: The common order-book dictionary for all exchanges.
        inverse: Whether to inverse the pair.
        debug: Whether to include debug info in the return book.
        aggLevels: Whether to aggregate same price levels."""
    ob = get_exch_book(exch, pair, obs)
    if not ob:
        return None
    if inverse:
        ob = ob.inverseBook(debug)
    else:
        if debug:
            ob.addLevelsDebug(pair)
    ob.pair = final_pair
    if exch in ob.xc.EXCHANGES + [f"{i}_jnd" for i in ob.xc.EXCHANGES]:
        # add fees
        ob.asks = ob.asksAfterFees(inverse=inverse)
        ob.bids = ob.bidsAfterFees(inverse=inverse)
    if aggLevels:
        ob.aggregateLevels(debug)
    return ob


def combo_book(
    pair: str,
    exch: str,
    obs: dict[str, list[OrderBookItem]],
    joinedPs: dict[str, tuple[str, str]] | None = None,
    debug: bool = False,
    aggLevels=False,
) -> list[OrderBookItem] | None:
    """Return combined order-book for given pair. Provides a super-set of the
    `combo_by_conversion` function, by also accounting for known pairs. So, if the
    given pair is a known pair, it will be returned as is.
    Args:
        pair: The pair to return the order-book for.
        exch: The exchange order-book to use for creating the combo book.
        obs: The common order-book dictionary for all exchanges.
        joinedPs: The pairs that have been joined."""
    books: list[OrderBookItem] | None = None
    _pair = pair
    inv_pair = "-".join(_pair.split("-")[::-1])
    if joinedPs:
        _pair = matchFromJoined(_pair, joinedPs)
        inv_pair = matchFromJoined(inv_pair, joinedPs)
    known_pairs = get_exch_obs_pairs(exch, obs)
    if _pair in known_pairs:
        # get known pair
        print(f"combo_book - Using known pair: {_pair}")
        if ob := get_taker_book(
            pair, _pair, exch, obs, debug=debug, aggLevels=aggLevels
        ):
            books = [ob]
    elif inv_pair in known_pairs:
        # get known pair and inverse it
        print(f"combo_book - Using inverse pair: {inv_pair}")
        if ob := get_taker_book(
            pair,
            inv_pair,
            exch,
            obs,
            inverse=True,
            debug=debug,
            aggLevels=aggLevels,
        ):
            books = [ob]
    else:
        print(f"combo_book - Synthesizing pair: {pair}")
        books = combo_by_conversion(pair, exch, obs, known_pairs, debug, aggLevels)
    return books


def pairs_sanity_check(
    obs: dict[str, list[OrderBookItem]], exchanges: list[str]
) -> None:
    """Check that all exchanges have the same pairs."""
    exch_pairs = {i: get_exch_obs_pairs(i, obs) for i in exchanges}
    # check all pairs match for all exchanges using sets.
    for exch1, exch2 in combinations(exch_pairs.keys(), 2):
        if (diff := set(exch_pairs[exch1]) - set(exch_pairs[exch2])) or (
            diff2 := set(exch_pairs[exch2]) - set(exch_pairs[exch1])
        ):
            print(
                f"pairs_sanity_check - Pairs mismatch for {exch1} and {exch2}: "
                f"{diff} {diff2}"
            )
