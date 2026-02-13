from pathlib import Path
import pandas as pd

def main():
    out = Path("data/example")
    out.mkdir(parents=True, exist_ok=True)

    usgs = pd.DataFrame([
        {"usgs_site_no": "09498500", "station_name": "SANTA CRUZ RIVER AT TUCSON", "latitude": 32.22, "longitude": -110.97, "agency": "USGS", "site_url": "https://example.com/09498500"},
        {"usgs_site_no": "09520700", "station_name": "RILLITO CREEK NEAR TUCSON", "latitude": 32.30, "longitude": -110.92, "agency": "USGS", "site_url": "https://example.com/09520700"},
    ])
    usgs.to_csv(out / "usgs_example.csv", index=False)

    other = pd.DataFrame([
        {"station_id": "PIMA_001", "name": "SANTA CRUZ RIVER SITE 1", "lat": 32.18, "lon": -111.00, "operator": "Pima County", "status": "active"},
        {"station_id": "PIMA_002", "name": "SANTA CRUZ RIVER SITE 2", "lat": 32.25, "lon": -110.95, "operator": "Pima County", "status": "inactive"},
    ])
    other.to_csv(out / "county_example.csv", index=False)

    print("Wrote example files to data/example/")
    print("To test locally, copy them into data/raw/ and run scripts/run_pipeline.py")

if __name__ == "__main__":
    main()
