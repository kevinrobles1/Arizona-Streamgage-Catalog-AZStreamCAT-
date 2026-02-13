from pathlib import Path
import pandas as pd
import yaml

from .schema import STD, MASTER_COL_ORDER
from .utils import pick_first_col, normalize_site_id, to_float, to_date, clean_str
from .qa import build_qa_report, find_duplicates
from .geo import write_geo_outputs

SUPPORTED = {".csv", ".xlsx"}

def read_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    if path.suffix.lower() == ".xlsx":
        # read first sheet by default
        return pd.read_excel(path)
    raise ValueError(f"Unsupported file: {path.name}")

def standardize_one(df: pd.DataFrame, cfg: dict, source_name: str) -> pd.DataFrame:
    cands = cfg["column_candidates"]

    site_id_col = pick_first_col(df, cands["site_id"])
    operator_col = pick_first_col(df, cands["operator"])
    name_col = pick_first_col(df, cands["name"])
    lat_col = pick_first_col(df, cands["lat"])
    lon_col = pick_first_col(df, cands["lon"])
    datum_col = pick_first_col(df, cands["datum"])
    status_col = pick_first_col(df, cands["status"])
    start_col = pick_first_col(df, cands["start_date"])
    end_col = pick_first_col(df, cands["end_date"])
    url_col = pick_first_col(df, cands["url"])

    out = pd.DataFrame()
    out[STD.site_id] = df[site_id_col].apply(normalize_site_id) if site_id_col else ""
    out[STD.operator] = df[operator_col].apply(clean_str) if operator_col else ""
    out[STD.name] = df[name_col].apply(clean_str) if name_col else ""
    out[STD.lat] = df[lat_col].apply(to_float) if lat_col else None
    out[STD.lon] = df[lon_col].apply(to_float) if lon_col else None
    out[STD.datum] = df[datum_col].apply(clean_str) if datum_col else ""
    out[STD.status] = df[status_col].apply(clean_str) if status_col else ""
    out[STD.start_date] = df[start_col].apply(to_date) if start_col else pd.NaT
    out[STD.end_date] = df[end_col].apply(to_date) if end_col else pd.NaT
    out[STD.url] = df[url_col].apply(clean_str) if url_col else ""

    out["source_file"] = source_name

    # Drop empty IDs if present
    out.loc[out[STD.site_id] == "", STD.site_id] = None
    out = out.dropna(subset=[STD.site_id]).copy()

    return out

def load_config(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def run_pipeline(config_path: Path) -> dict:
    cfg = load_config(config_path)
    input_dir = Path(cfg["input_dir"])
    output_dir = Path(cfg["output_dir"])
    output_dir.mkdir(parents=True, exist_ok=True)

    globs = cfg.get("input_globs", ["*.csv", "*.xlsx"])

    paths = []
    for g in globs:
        paths.extend(sorted(input_dir.glob(g)))

    paths = [p for p in paths if p.suffix.lower() in SUPPORTED]

    if not paths:
        raise RuntimeError(f"No input files found in {input_dir}")

    parts = []
    for p in paths:
        df = read_any(p)
        std = standardize_one(df, cfg, source_name=p.name)
        parts.append(std)

    master = pd.concat(parts, ignore_index=True)

    # Reorder
    for col in MASTER_COL_ORDER:
        if col not in master.columns:
            master[col] = None
    master = master[MASTER_COL_ORDER + ["source_file"]].copy()

    # QA
    qa = build_qa_report(master, cfg)
    dups = find_duplicates(master)

    # Save outputs
    master_path = output_dir / cfg["outputs"]["master_table"]
    qa_path = output_dir / cfg["outputs"]["qa_report"]
    dups_path = output_dir / cfg["outputs"]["duplicates"]

    master.to_csv(master_path, index=False)
    qa.to_csv(qa_path, index=False)
    dups.to_csv(dups_path, index=False)

    # Geo outputs (optional)
    geo_out = output_dir / cfg["outputs"]["geojson"]
    write_geo_outputs(master, geo_out, cfg)

    return {
        "master_csv": str(master_path),
        "qa_csv": str(qa_path),
        "duplicates_csv": str(dups_path),
        "geojson": str(geo_out),
        "rows_master": int(len(master)),
        "rows_duplicates": int(len(dups)),
        "qa_rows": int(len(qa)),
    }
