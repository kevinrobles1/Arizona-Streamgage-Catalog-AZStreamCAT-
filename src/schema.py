from dataclasses import dataclass

@dataclass
class StandardColumns:
    site_id: str = "site_id"
    operator: str = "operator"
    name: str = "name"
    lat: str = "lat"
    lon: str = "lon"
    datum: str = "datum"
    status: str = "status"
    start_date: str = "start_date"
    end_date: str = "end_date"
    url: str = "url"

STD = StandardColumns()

MASTER_COL_ORDER = [
    STD.site_id,
    STD.operator,
    STD.name,
    STD.lat,
    STD.lon,
    STD.datum,
    STD.status,
    STD.start_date,
    STD.end_date,
    STD.url,
]
