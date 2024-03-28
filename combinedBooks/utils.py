"""
Tools for combined order books research in crypto space. Utils functions.
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

import json
from datetime import datetime, timezone
from pathlib import Path


def nowUTCts() -> float:
    return round(datetime.now(tz=timezone.utc).timestamp(), 3)


def saveEveryNth(results: list, data_dir: str, filename: str, nRes: int) -> bool:
    _data_dir = Path(data_dir)
    if not _data_dir.exists():
        _data_dir.mkdir(parents=True)
    filepath = _data_dir / filename
    saved = False
    if (n_res := len(results)) >= nRes:
        if filepath.exists():
            try:
                with open(filepath, "r") as infile:
                    res = json.load(infile)
                    res += results
            except json.decoder.JSONDecodeError as der:
                print(f"Decoder error {der}")
                res = results
            with open(filepath, "w") as outfile:
                json.dump(res, outfile)
            print(f"Saved {len(res)} results")
            saved = True
        else:
            with open(filepath, "w") as outfile:
                json.dump(results, outfile)
            print(f"Saved {n_res} results")
            saved = True
    return saved


def round_digits(precision_a: int, precision_b: int, num: float) -> int:
    """Return suggested number of decimal places to round a number to based on the
    precision of the inputs and the result"""
    init_precision = max(precision_a, precision_b)
    if 1 > num > 1e-2:
        precision = 5
    elif num <= 1e-2:
        precision = 8
    else:
        precision = 2
    return max(precision, init_precision)
