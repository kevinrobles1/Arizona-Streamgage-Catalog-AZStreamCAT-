# Collects all CoCoRaHS station data for one or all states
# Exports Excel files in the WY SEO format
# Years_Op is always a numeric value (no 'unavailable')

"""
HOW TO RUN THIS SCRIPT (Step-by-step)
# Make sure u have Python 3.8 or newer. 
# Install required packages: pip install requests pandas openpyxl
# Save this file as: internship_v22_all_comments_fixed_years.py
# Open a terminal or command prompt in the folder where this file is saved
# Run the script: python internship_v22_all_comments_fixed_years.py
# When asked, type a full state name (like Arizona) or type ALL
# Excel files will be saved to the folder set in SAVE_DIR
# If an Excel file is open, the script saves a new copy with a number (_2, _3, etc.)
# Since this would be done in a different computer, update the folder path in the line starting with SAVE_DIR = ( which is in line 44). 
# This automatically saves the files to the user’s own Documents folder on any computer.
"""

import os  # helps with folder and file paths on your computer
import time  # lets the program pause for a moment so we are polite to the website
import requests  # gets data from the internet API
import pandas as pd  # puts the data into tables and saves Excel files
from datetime import datetime  # works with dates and times

STATE_MAP = {  # turns full state names into two letter codes
    "Alabama": "AL", "Alaska": "AK", "Arizona": "AZ", "Arkansas": "AR", "California": "CA",
    "Colorado": "CO", "Connecticut": "CT", "Delaware": "DE", "Florida": "FL", "Georgia": "GA",
    "Hawaii": "HI", "Idaho": "ID", "Illinois": "IL", "Indiana": "IN", "Iowa": "IA",
    "Kansas": "KS", "Kentucky": "KY", "Louisiana": "LA", "Maine": "ME", "Maryland": "MD",
    "Massachusetts": "MA", "Michigan": "MI", "Minnesota": "MN", "Mississippi": "MS", "Missouri": "MO",
    "Montana": "MT", "Nebraska": "NE", "Nevada": "NV", "New Hampshire": "NH", "New Jersey": "NJ",
    "New Mexico": "NM", "New York": "NY", "North Carolina": "NC", "North Dakota": "ND",
    "Ohio": "OH", "Oklahoma": "OK", "Oregon": "OR", "Pennsylvania": "PA", "Rhode Island": "RI",
    "South Carolina": "SC", "South Dakota": "SD", "Tennessee": "TN", "Texas": "TX", "Utah": "UT",
    "Vermont": "VT", "Virginia": "VA", "Washington": "WA", "West Virginia": "WV", "Wisconsin": "WI",
    "Wyoming": "WY", "District of Columbia": "DC", "Puerto Rico": "PR"
}

API_URL = "https://functions-dev-dex-cocorahs-org.azurewebsites.net/api/StationHistoryReport"  # website address for the data

SAVE_DIR = r"C:\Users\roble\OneDrive\Documents\AZStreamCAT- Internship"  # folder path where the Excel file will be saved

def safe(value):  # makes empty things show the word unavailable
    if value in [None, "", " "]:  # checks if the value is missing
        return "unavailable"  # uses the word unavailable so the cell is not blank
    else:
        return value  # keeps the original value if it is there

def parse_date_safe(date_str):  # tries to turn a text date into a real date
    if not date_str:  # if there is no date at all
        return None  # returns nothing
    try:
        return datetime.strptime(date_str.split("T")[0], "%Y-%m-%d")  # keeps only YYYY-MM-DD part
    except Exception:
        return None  # gives up if the date is not readable

def get_api_page(state_code, skip):  # gets one page of stations from the API for one state
    params = {
        "country": "usa",
        "state": state_code,
        "skip": skip,
        "take": 100,
        "sortfield": "stationNumber",
        "sortdir": "asc"
    }  # settings we send to the API
    headers = {"User-Agent": "Mozilla/5.0"}  # makes the request look like a normal web browser
    r = requests.get(API_URL, params=params, headers=headers, timeout=60)  # sends the request to the API
    r.raise_for_status()  # stops the program if the website returns an error
    return r.json()  # gives back the data as a Python dictionary

def clean_record(rec, state_full):  # turns one raw station record into the exact columns we need
    raw_status = str(rec.get("statusName", "")).lower().strip()  # gets the status text and makes it lowercase
    if "active" in raw_status or "report" in raw_status:  # if the text says active or reporting
        status = "active"  # we mark it as active
    else:
        status = "inactive"  # everything else is inactive

    d1 = parse_date_safe(rec.get("firstObsDate"))  # first day the station reported
    d2 = parse_date_safe(rec.get("lastObsDate"))  # last day the station reported

    if not d2:  # if the last day is missing
        d2 = datetime.now()  # use today for the last day
    if not d1:  # if the first day is missing
        d1 = datetime.now()  # use today for the first day

    try:
        delta_years = (d2 - d1).days / 365.25  # number of years between first and last day
        if delta_years < 0:  # if dates are backwards
            years_op = 0  # set to zero years
        elif delta_years < 1:  # if less than one full year
            years_op = 1.0  # count it as one year
        else:
            years_op = round(delta_years, 1)  # round to one decimal place
    except Exception:
        years_op = 0  # if math fails we use zero

    return {
        "Agency": "CoCoRaHS",
        "Station_ID": safe(rec.get("stationNumber")),
        "Station_Name": safe(rec.get("stationName")),
        "Watershed": "",
        "County": safe(rec.get("county")),
        "AMA_INA": "",
        "Latitude": safe(rec.get("latitude")),
        "Longitude": safe(rec.get("longitude")),
        "Elevation_ft": safe(rec.get("elevation")),
        "State": state_full,
        "Start_Record": d1.date().isoformat(),
        "End_Record": d2.date().isoformat(),
        "Years_Op": years_op,
        "Measurement_Type": "precipitation",
        "Status": status,
        "Measurement_Freq": "unavailable",
        "Gage_type": "precipitation",
        "Link": f"https://dex.cocorahs.org/stations/{rec.get('stationId')}",
        "Agency_Type": "state",
        "Notes": ""
    }  # builds the final row for Excel

def build_one_state(state_full, county_filter=None):  # makes and saves one Excel file for one state (optionally one county)
    state_code = STATE_MAP.get(state_full)  # gets the two letter code like AZ
    if not state_code:  # if we cannot find the state
        print("State not recognized.")  # tells the user there is a problem
        return  # stop here

    print("Getting all stations for", state_full, "(", state_code, ")...")  # lets u know we started
    all_records = []  # will hold every station record we get
    skip = 0  # starts at the first page

    while True:  # keeps going until there are no more pages
        data = get_api_page(state_code, skip)  # gets one page of data
        items = data.get("items", [])  # pulls out the list of stations
        if not items:  # if the list is empty
            break  # stop the loop
        all_records.extend(items)  # add these stations to our list
        skip += 100  # move to the next page
        print("  processed", len(all_records), "stations...")
        if not data.get("hasNext", False):  # if there is no next page
            break  # stop the loop
        time.sleep(0.3)  # pause a tiny bit 

    if not all_records:  # if we did not get any stations
        print("No stations found.")  # tell nothing was found
        return  

    cleaned = [clean_record(r, state_full) for r in all_records]  # cleans every station into the right columns
    df = pd.DataFrame(cleaned)  # puts the cleaned rows into a table

    # If a county filter is given, keep only that county (case-insensitive)
    if county_filter:  # if the user asked for a specific county
        # normalize both sides so spelling/case differences do not break it
        county_norm = county_filter.strip().lower()
        df = df[df["County"].str.strip().str.lower() == county_norm]

        if df.empty:  # if no stations matched that county
            print(f"No stations found for county '{county_filter}' in {state_full}.")
            return  # stop here

    col_order = [
        "Agency", "Station_ID", "Station_Name", "Watershed", "County", "AMA_INA",
        "Latitude", "Longitude", "Elevation_ft", "State", "Start_Record", "End_Record",
        "Years_Op", "Measurement_Type", "Status", "Measurement_Freq", "Gage_type",
        "Link", "Agency_Type", "Notes"
    ]  # the exact column order needed by WY SEO
    df = df[col_order]  # puts the columns in that exact order

    os.makedirs(SAVE_DIR, exist_ok=True)  # makes the folder if it does not already exist

    # Build file name: include county if used, otherwise just state
    if county_filter:
        # replace spaces in county with underscores so the file name is clean
        county_clean = county_filter.replace(" ", "_")
        base_path = os.path.join(SAVE_DIR, f"{state_full}_{county_clean}_CoCoRaHS_FULL.xlsx")
    else:
        base_path = os.path.join(SAVE_DIR, f"{state_full}_CoCoRaHS_FULL.xlsx")

    out_path = base_path  # starts with the main file name
    version = 2  # used if the file is open and we need a new number

    while True:  # tries to save the file and makes a new number if needed
        try:
            df.to_excel(out_path, index=False)  # saves the Excel file without row numbers
            break  # done saving
        except PermissionError:
            # new file name with a number if Excel has the file locked
            if county_filter:
                county_clean = county_filter.replace(" ", "_")
                out_path = os.path.join(
                    SAVE_DIR,
                    f"{state_full}_{county_clean}_CoCoRaHS_FULL_{version}.xlsx"
                )
            else:
                out_path = os.path.join(
                    SAVE_DIR,
                    f"{state_full}_CoCoRaHS_FULL_{version}.xlsx"
                )
            version += 1  # go to the next number

    if county_filter:
        print(
            state_full,
            "- county:",
            county_filter,
            ":",
            len(df),
            "stations processed and saved to",
            out_path,
        )
    else:
        print(state_full, ":", len(df), "stations processed and saved to", out_path)  # final message 

def main():  # the main starting point of the program
    choice = input("Type full state name (e.g., Arizona) or 'ALL': ").strip()  # asks what to run

    if choice.lower() == "all":  # if the user wants all states
        for state in STATE_MAP.keys():  # goes through every state name
            build_one_state(state)  # makes the file for that state
    else:
        # user chose a single state, now ask if they also want a specific county
        county_choice = input(
            "Type county name for this state (or press Enter for ALL counties in that state): "
        ).strip()

        if county_choice == "":  # if user just hits Enter
            county_choice = None  # means do all counties in that state

        build_one_state(choice, county_filter=county_choice)  # makes the file for this state (and maybe county)

if __name__ == "__main__":  
    main(
