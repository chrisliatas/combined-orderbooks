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
