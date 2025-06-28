"""
ETL for the *full* 10-file Georgia SDWIS Q1-2025 drop.

    python ingest_data.py

Differences vs v-1
• Understands every CSV listed in the new README
• Uses the primary-key definitions from the spec (see KEY_MAP below)
• Creates two convenience SQL views (water_systems, violations) so the
  old Streamlit UI keeps working unchanged.
"""
import duckdb, sqlite3, pathlib, re, pandas as pd, datetime as dt
from sqlite_utils import Database
from utils import slug

RAW_DIR = pathlib.Path("data")
DB_FILE = "georgia_water.db"

# ──────────────────── 1.  PKs per table  ────────────────────
KEY_MAP = {
    "events_milestones"        : ["submissionyearquarter", "pwsid", "event_schedule_id"],
    "facilities"               : ["submissionyearquarter", "pwsid", "facility_id"],
    "geographic_areas"         : ["submissionyearquarter", "pwsid", "geo_id"],
    "lcr_samples"              : ["submissionyearquarter", "pwsid", "sar_id"],
    "pn_violation_assoc"       : ["pn_violation_id"],
    "pub_water_systems"        : ["submissionyearquarter", "pwsid"],
    "ref_ansi_areas"           : ["ansi_state_code", "ansi_entity_code"],
    "ref_code_values"          : ["value_type", "value_code"],
    "service_areas"            : ["submissionyearquarter", "pwsid", "service_area_type_code"],
    "site_visits"              : ["submissionyearquarter", "pwsid", "visit_id"],
    "violations_enforcement"   : ["violation_id"],
}

# Any column that *looks* like a date will be parsed into ISO 8601
DATE_COL_PATTERN = re.compile(r"_date$|_date_|_dt$|_dt_", re.I)

# ──────────────────── 2.  Load with DuckDB  ──────────────────
duck = duckdb.connect()
for csv in RAW_DIR.glob("SDWA_*.csv"):
    view = slug(csv.stem.replace("SDWA_", ""))
    duck.execute(
        f"CREATE OR REPLACE VIEW {view} AS SELECT * FROM read_csv_auto('{csv}', header=True)"
    )

# ──────────────────── 3.  Copy to SQLite  ────────────────────
db = Database(sqlite3.connect(DB_FILE))

for view in duck.execute("SHOW TABLES").fetchall():
    view = view[0]
    df = duck.execute(f"SELECT * FROM {view}").df()
    df.columns = [slug(c) for c in df.columns]

    # Light-weight date coercion
    for c in df.columns:
        if DATE_COL_PATTERN.search(c):
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    pk = KEY_MAP.get(view)
    db[view].insert_all(df.to_dict("records"), pk=pk, alter=True, replace=True, batch_size=5000)
    if pk:
        db[view].create_index(pk)

    print(f"✔ {view:<24} {len(df):>8,} rows  pk={pk}")

# ──────────────────── 4.  FTS + “legacy” views  ──────────────
db["pub_water_systems"].enable_fts(["pws_name"], replace=True)

# keep old UI queries working ↓
db.conn.executescript("""
    DROP VIEW IF EXISTS water_systems;
    DROP VIEW IF EXISTS violations;
    CREATE VIEW water_systems AS
        SELECT * FROM pub_water_systems;
    CREATE VIEW violations  AS
        SELECT * FROM violations_enforcement;
""")

print("\n🎉  georgia_water.db rebuilt for the Q1-2025 dataset")