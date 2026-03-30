"""
JSON Cleaner — converts all non-serializable types to JSON-safe values.
Apply to any result before returning from an endpoint.
"""

import datetime
import numpy as np
import pandas as pd


def clean_for_json(obj):
    """Recursively convert all non-JSON-serializable types."""
    if obj is None:
        return None
    
    # Handle NaN/NaT
    if isinstance(obj, float) and np.isnan(obj):
        return None
    
    # pandas Timestamp
    if isinstance(obj, pd.Timestamp):
        if pd.isna(obj):
            return None
        return obj.isoformat()
    
    # pandas NaT
    if obj is pd.NaT:
        return None
    
    # datetime
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    
    # numpy types
    if isinstance(obj, (np.integer,)):
        return int(obj)
    if isinstance(obj, (np.floating,)):
        if np.isnan(obj):
            return None
        return float(obj)
    if isinstance(obj, np.bool_):
        return bool(obj)
    if isinstance(obj, np.ndarray):
        return [clean_for_json(x) for x in obj.tolist()]
    
    # bytes
    if isinstance(obj, bytes):
        return None
    
    # sets
    if isinstance(obj, set):
        return [clean_for_json(x) for x in obj]
    
    # dict
    if isinstance(obj, dict):
        return {str(k): clean_for_json(v) for k, v in obj.items()}
    
    # list/tuple
    if isinstance(obj, (list, tuple)):
        return [clean_for_json(x) for x in obj]
    
    # pandas Series/DataFrame
    if isinstance(obj, pd.Series):
        return clean_for_json(obj.tolist())
    if isinstance(obj, pd.DataFrame):
        return clean_for_json(obj.to_dict(orient='records'))
    
    # Regular serializable types
    if isinstance(obj, (str, int, float, bool)):
        return obj
    
    # Fallback: convert to string
    try:
        return str(obj)
    except:
        return None
