from pathlib import Path
import pandas as pd
from .schema import STD

def write_geo_outputs(master: pd.DataFrame, geojson_path: Path, cfg: dict) -> None:
    try:
        import geopandas as gpd
        from shapely.geometry import Point
    except Exception:
        return

    df = master.dropna(subset=[STD.lat, STD.lon]).copy()
    if len(df) == 0:
        return

    df["geometry"] = df.apply(lambda r: Point(float(r[STD.lon]), float(r[STD.lat])), axis=1)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    epsg = int(cfg.get("export_crs_epsg", 4326))
    if epsg != 4326:
        gdf = gdf.to_crs(epsg=epsg)

    geojson_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(geojson_path, driver="GeoJSON")
