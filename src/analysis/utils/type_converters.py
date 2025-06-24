"""Type conversion utilities for data analysis."""

from datetime import datetime, time, date
import pandas as pd
import polars as pl
import numpy as np
from typing import Any, Dict
import json
from logging import getLogger

logger = getLogger(__name__)

class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder for datetime objects."""
    def default(self, obj):
        if isinstance(obj, (datetime, time, date)):
            return obj.isoformat()
        return super().default(obj)

def convert_polars_types(obj: Any) -> Any:
    """Convert Polars types to Python native types for JSON serialization."""
    if isinstance(obj, (pl.Expr, pl.Series)):
        return obj.to_list()
    if isinstance(obj, pl.DataFrame):
        return obj.to_pandas().to_dict()
    if isinstance(obj, dict):
        return {k: convert_polars_types(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [convert_polars_types(v) for v in obj]
    if isinstance(obj, (pl.Datetime, pl.Date)):
        return obj.strftime("%Y-%m-%d %H:%M:%S") if isinstance(obj, pl.Datetime) else obj.strftime("%Y-%m-%d")
    if isinstance(obj, pd.Timestamp):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(obj, (datetime, time, date)):
        return obj.isoformat()
    if isinstance(obj, pd.DatetimeTZDtype):
        return str(obj)
    if isinstance(obj, (pl.Float32, pl.Float64)):
        return float(obj)
    if isinstance(obj, (pl.Int32, pl.Int64)):
        return int(obj)
    if isinstance(obj, pl.Boolean):
        return bool(obj)
    if pd.isna(obj):
        return None
    return obj
