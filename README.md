# CoCoRaHS Station Export to Excel 

This script pulls station history data from the CoCoRaHS DEX API for one US state (or all states) and exports the results to Excel files in the WY SEO format.

It also supports an optional county filter for a single state.

## What it produces

For each state you export, the script creates an Excel file that includes columns like:

- Agency, Station_ID, Station_Name
- County, Latitude, Longitude, Elevation_ft, State
- Start_Record, End_Record, Years_Op
- Status (active/inactive)
- Measurement_Type, Measurement_Freq, Gage_type
- Link (DEX station page)

### Notes about key fields

- `Years_Op` is always numeric (never "unavailable").
  - If the date range is under 1 year, it is set to `1.0`.
  - If the dates are reversed or missing, it falls back to `0` or uses today’s date to compute.
- `Status` is set to `active` if `statusName` contains “active” or “report”, otherwise `inactive`.
- Missing values are filled with `"unavailable"` for most text fields.

## Requirements

- Python 3.8+
- Packages:
  - requests
  - pandas
  - openpyxl

Install:

```bash
pip install requests pandas openpyxl
