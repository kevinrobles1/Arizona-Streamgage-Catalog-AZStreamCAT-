import re
import pandas as pd

def clean_str(x):
    if pd.isna(x):
        return ""
    return str(x).strip()

def normalize_site_id(x: str) -> str:
    s = clean_str(x)
    s = re.sub(r"\s+", "", s)
    return s

def to_float(x):
    try:
        if pd.isna(x):
            return None
        return float(x)
    except Exception:
        return None

def to_date(x):
    if pd.isna(x) or str(x).strip() == "":
        return pd.NaT
    return pd.to_datetime(x, errors="coerce")

def pick_first_col(df, candidates):
    cols = {c.lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower()
        if key in cols:
            return cols[key]
    return None
