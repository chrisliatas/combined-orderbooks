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

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(parents=True, exist_ok=True)


def nowUTCts() -> float:
    return round(datetime.now(tz=timezone.utc).timestamp(), 3)


def saveEveryNth(results: list, filename: str, nRes: int) -> bool:
    filepath = DATA_DIR / filename
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
