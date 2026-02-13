import pandas as pd
from rapidfuzz import fuzz

from .schema import STD

def in_arizona_bbox(lat, lon) -> bool:
    # Rough AZ bounding box (good enough for QA flagging)
    if lat is None or lon is None:
        return False
    return (31.0 <= lat <= 37.5) and (-115.2 <= lon <= -108.7)

def build_qa_report(master: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    rules = cfg.get("rules", {})
    flag_bbox = bool(rules.get("flag_if_coords_outside_az", True))

    rows = []

    missing_coords = master[master[STD.lat].isna() | master[STD.lon].isna()]
    if len(missing_coords) > 0:
        rows.append({"check": "missing_coords", "count": int(len(missing_coords))})

    if flag_bbox:
        bad = master.dropna(subset=[STD.lat, STD.lon]).copy()
        bad["in_az"] = bad.apply(lambda r: in_arizona_bbox(r[STD.lat], r[STD.lon]), axis=1)
        outside = bad[~bad["in_az"]]
        if len(outside) > 0:
            rows.append({"check": "coords_outside_az_bbox", "count": int(len(outside))})

    missing_operator = master[master[STD.operator].fillna("").str.strip() == ""]
    if len(missing_operator) > 0:
        rows.append({"check": "missing_operator", "count": int(len(missing_operator))})

    missing_name = master[master[STD.name].fillna("").str.strip() == ""]
    if len(missing_name) > 0:
        rows.append({"check": "missing_name", "count": int(len(missing_name))})

    if not rows:
        rows.append({"check": "no_issues_found", "count": 0})

    return pd.DataFrame(rows)

def find_duplicates(master: pd.DataFrame) -> pd.DataFrame:
    # Basic duplicate: same site_id appears multiple times (common across agencies)
    d = master[master[STD.site_id].duplicated(keep=False)].copy()
    if len(d) == 0:
        return d

    d = d.sort_values([STD.site_id, STD.operator, "source_file"]).reset_index(drop=True)
    return d

def fuzzy_name_match(a: str, b: str) -> int:
    return fuzz.token_sort_ratio(a or "", b or "")
