import argparse
import time
from pathlib import Path

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

URL = "https://www.cocorahs.org/Stations/ListStations.aspx"

STATION_COL_CANDIDATES = ["stationnumber", "StationID", "station", "station_id", "Station_ID"]
DATE_COL_CANDIDATES = ["reportdate", "date", "ReportDate", "Date"]

def make_driver(headless: bool) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1400,900")
    opts.add_experimental_option("excludeSwitches", ["enable-automation", "enable-logging"])
    opts.add_experimental_option("useAutomationExtension", False)
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    return driver

def save_debug(driver: webdriver.Chrome, outdir: Path, tag: str) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    (outdir / f"{tag}.html").write_text(driver.page_source, encoding="utf-8")
    try:
        driver.save_screenshot(str(outdir / f"{tag}.png"))
    except Exception:
        pass

def wait_for_form(driver: webdriver.Chrome, wait: WebDriverWait):
    wait.until(lambda d: d.execute_script("return document.readyState") in ("interactive", "complete"))
    time.sleep(1.0)

    # Basic selectors (IDs usually stable)
    country_locators = [
        (By.ID, "ddlCountry"),
        (By.XPATH, "//select[contains(@id,'ddlCountry')]"),
    ]
    state_locators = [
        (By.ID, "ddlState"),
        (By.XPATH, "//select[contains(@id,'ddlState')]"),
    ]
    search_locators = [
        (By.ID, "btnSearch"),
        (By.XPATH, "//input[@type='submit' and (contains(@id,'btnSearch') or contains(@value,'Search'))]"),
        (By.XPATH, "//button[contains(.,'Search')]"),
    ]

    country = state = search_btn = None

    for loc in country_locators:
        try:
            country = wait.until(EC.presence_of_element_located(loc))
            break
        except Exception:
            pass

    for loc in state_locators:
        try:
            state = wait.until(EC.presence_of_element_located(loc))
            break
        except Exception:
            pass

    for loc in search_locators:
        try:
            search_btn = wait.until(EC.element_to_be_clickable(loc))
            break
        except Exception:
            pass

    if not (country and state and search_btn):
        raise RuntimeError("Could not find country/state/search controls")

    return country, state, search_btn

def find_stations_table(driver: webdriver.Chrome, wait: WebDriverWait):
    locators = [
        (By.ID, "dgStations"),
        (By.XPATH, "//table[contains(@id,'dgStations')]"),
        (By.XPATH, "//table[.//tr/th and .//tr/td]"),
    ]
    for loc in locators:
        try:
            return wait.until(EC.presence_of_element_located(loc))
        except Exception:
            pass
    raise RuntimeError("Stations table not detected")

def scrape_stations(state_name: str, headless: bool, debug_dir: Path) -> pd.DataFrame:
    driver = make_driver(headless=headless)
    wait = WebDriverWait(driver, 45)

    try:
        driver.get(URL)

        tries = 0
        while True:
            tries += 1
            try:
                country_el, state_el, search_btn = wait_for_form(driver, wait)
                Select(country_el).select_by_visible_text("United States")
                Select(state_el).select_by_visible_text(state_name)
                search_btn.click()
                break
            except Exception:
                if tries >= 2:
                    save_debug(driver, debug_dir, "form_debug")
                    raise
                time.sleep(2)
                driver.refresh()

        table = find_stations_table(driver, wait)

        rows_all = []
        page = 1
        while True:
            # Table DOM can refresh after paging
            table = wait.until(EC.presence_of_element_located((By.XPATH, "//table[.//tr/th and .//tr/td]")))
            trs = table.find_elements(By.XPATH, ".//tr[td]")

            for tr in trs:
                tds = tr.find_elements(By.TAG_NAME, "td")
                if len(tds) >= 3:
                    station_id = tds[0].text.strip()
                    name = tds[1].text.strip()
                    county = tds[2].text.strip()
                    if station_id:
                        rows_all.append(
                            {"StationID": station_id, "Name": name, "County": county, "State": state_name}
                        )

            # Look for Next link
            next_link = None
            try:
                next_link = table.find_element(By.XPATH, ".//a[contains(.,'Next')]")
            except Exception:
                next_link = None

            if not next_link:
                break

            try:
                driver.execute_script("arguments[0].scrollIntoView({block:'center'});", next_link)
                next_link.click()
            except Exception:
                driver.execute_script("arguments[0].click();", next_link)

            page += 1
            time.sleep(1.0)

        df = pd.DataFrame(rows_all).drop_duplicates()
        return df

    except Exception:
        save_debug(driver, debug_dir, "grid_debug")
        raise

    finally:
        try:
            driver.quit()
        except Exception:
            pass

def choose_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None

def make_timeline(stations_df: pd.DataFrame, input_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(input_csv)
    st_col = choose_col(df, STATION_COL_CANDIDATES)
    dt_col = choose_col(df, DATE_COL_CANDIDATES)
    if not st_col or not dt_col:
        raise RuntimeError("Input CSV must contain a station id column and a date column")

    df[dt_col] = pd.to_datetime(df[dt_col], errors="coerce")
    df = df.dropna(subset=[st_col, dt_col])
    df[st_col] = df[st_col].astype(str).str.strip()

    stations_df = stations_df.copy()
    stations_df["StationID"] = stations_df["StationID"].astype(str).str.strip()

    tl = df.groupby(st_col)[dt_col].agg(earliest="min", latest="max").reset_index()
    merged = (
        stations_df.merge(tl, left_on="StationID", right_on=st_col, how="inner")
        .drop(columns=[st_col])
        .sort_values(["earliest", "StationID"])
        .reset_index(drop=True)
    )
    return merged[["StationID", "Name", "County", "State", "earliest", "latest"]]

def main():
    ap = argparse.ArgumentParser(description="Scrape CoCoRaHS stations for a state, optional timeline merge.")
    ap.add_argument("--state", default="California", help="State name as shown in the dropdown (example: California)")
    ap.add_argument("--outdir", default="outputs", help="Output folder")
    ap.add_argument("--headless", action="store_true", help="Run Chrome headless")
    ap.add_argument("--timeline-from", default=None, help="Optional CSV to compute earliest and latest dates per station")
    args = ap.parse_args()

    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    debug_dir = outdir / "debug"

    stations = scrape_stations(args.state, headless=args.headless, debug_dir=debug_dir)
    stations_path = outdir / f"cocorahs_{args.state.replace(' ', '_')}_stations.csv"
    stations.to_csv(stations_path, index=False)
    print(f"[OK] Saved stations: {stations_path} (rows: {len(stations)})")

    if args.timeline_from:
        timeline = make_timeline(stations, Path(args.timeline_from))
        timeline_path = outdir / f"cocorahs_{args.state.replace(' ', '_')}_timeline.csv"
        timeline.to_csv(timeline_path, index=False)
        print(f"[OK] Saved timeline: {timeline_path} (rows: {len(timeline)})")

if __name__ == "__main__":
    main()
