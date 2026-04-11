"""
Data pipeline for fetching Statcast pitch-level data from Baseball Savant.

Uses pybaseball, which queries the official Baseball Savant CSV export endpoint
(/statcast_search/csv) — no scraping involved. Results are cached as Parquet
files on disk so subsequent runs avoid redundant downloads.

Cache policy:
  - Past years: loaded from data/cache/statcast_{year}.parquet if it exists.
  - Current year: always re-fetched, since data updates daily mid-season.
"""

from datetime import date
from pathlib import Path

import pandas as pd
import pybaseball

from config.settings import DataSettings

RAW_DIR = Path("data/cache")
FULL_DIR = Path("data/full")
FULL_PATH = FULL_DIR / "statcast.parquet"

pybaseball.cache.enable()


def _raw_path(year: int) -> Path:
    return RAW_DIR / f"statcast_{year}.parquet"


def _fetch_year(year: int) -> pd.DataFrame:
    """Download a full season of Statcast data via the Baseball Savant CSV endpoint."""
    start = f"{year}-01-01"
    end = f"{year}-12-31"
    return pybaseball.statcast(start_dt=start, end_dt=end, verbose=False)


def fetch_and_cache(settings: DataSettings | None = None) -> None:
    """
    Download Statcast data for each year in [start_year, end_year] and write
    each year to data/cache/statcast_{year}.parquet.

    Past years are skipped if a cache file already exists. The current year is
    always re-fetched since Baseball Savant updates daily during the season.
    """
    if settings is None:
        settings = DataSettings()

    current_year = date.today().year
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    for year in range(settings.start_year, settings.end_year + 1):
        path = _raw_path(year)
        is_current = year == current_year

        if path.exists() and not is_current:
            continue

        df = _fetch_year(year)
        if df is not None and not df.empty:
            df.to_parquet(path, index=False)


def combine(settings: DataSettings | None = None) -> pd.DataFrame:
    """
    Read each year's cached Parquet file, combine them into a single DataFrame,
    and write it to data/full/statcast.parquet.

    Returns the combined DataFrame.
    """
    if settings is None:
        settings = DataSettings()

    FULL_DIR.mkdir(parents=True, exist_ok=True)

    frames: list[pd.DataFrame] = []
    for year in range(settings.start_year, settings.end_year + 1):
        path = _raw_path(year)
        if not path.exists():
            continue
        frames.append(pd.read_parquet(path))

    if not frames:
        print("No data to combine.")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    combined["game_date"] = pd.to_datetime(combined["game_date"])
    combined.sort_values("game_date", inplace=True, ignore_index=True)

    combined.to_parquet(FULL_PATH, index=False)

    return combined


def end_to_end(settings: DataSettings | None = None) -> pd.DataFrame:
    """Fetch, cache, and combine Statcast data in one shot."""
    fetch_and_cache(settings)
    return combine(settings)
