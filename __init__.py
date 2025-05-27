"""AMFI Data Fetcher package for retrieving mutual fund data."""

from .data_retrieve import (
    fetch_amfi_data,
    process_data,
    get_last_5_years_data,
    save_to_json,
    main,
    SchemeData,
    DailyData
)

__version__ = "0.1.0"
__all__ = [
    "fetch_amfi_data",
    "process_data",
    "get_last_5_years_data",
    "save_to_json",
    "main",
    "SchemeData",
    "DailyData"
] 