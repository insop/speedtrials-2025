"""
ETL for the GA-SDWIS Q1-2025 10-file drop.

    python ingest_data.py
"""
import duckdb, sqlite3, pathlib, re, pandas as pd
from sqlite_utils import Database

# ------------------------------------------------------------------ #
# 1 . helper – keep working even if utils.py is missing
# ------------------------------------------------------------------ #
try:
    from utils import slug           # original nicest helper
except ModuleNotFoundError:
    def slug(txt: str) -> str:       # quick inline fallback
        return re.sub(r"[^0-9a-z]+", "_", txt.lower()).strip("_")

RAW_DIR = pathlib.Path("data")
DB_FILE = "georgia_water.db"

# primary-key definitions drawn from the README
KEY_MAP = {
    "events_milestones"      : ["submissionyearquarter", "pwsid", "event_schedule_id"],
    "facilities"             : ["submissionyearquarter", "pwsid", "facility_id"],
    "geographic_areas"       : ["submissionyearquarter", "pwsid", "geo_id"],
    "lcr_samples"            : ["submissionyearquarter", "pwsid", "sar_id"],
    "pn_violation_assoc"     : ["pn_violation_id"],
    "pub_water_systems"      : ["submissionyearquarter", "pwsid"],
    "ref_ansi_areas"         : ["ansi_state_code", "ansi_entity_code"],
    "ref_code_values"        : ["value_type", "value_code"],
    "service_areas"          : ["submissionyearquarter", "pwsid", "service_area_type_code"],
    "site_visits"            : ["submissionyearquarter", "pwsid", "visit_id"],
    "violations_enforcement" : ["violation_id"],
}

DATE_COL_RE = re.compile(r"_date$|_date_|_dt$|_dt_", re.I)

# ------------------------------------------------------------------ #
# 2 . mount every CSV as a DuckDB view (fast!)
# ------------------------------------------------------------------ #
duck = duckdb.connect()
for csv in RAW_DIR.glob("SDWA_*.csv"):
    view = slug(csv.stem.replace("SDWA_", ""))
    duck.execute(
        f"CREATE OR REPLACE VIEW {view} AS "
        f"SELECT * FROM read_csv_auto('{csv}', header=True)"
    )

# ------------------------------------------------------------------ #
# 3 . copy to SQLite, making dates safe for sqlite-utils
# ------------------------------------------------------------------ #
db = Database(sqlite3.connect(DB_FILE))

for (view,) in duck.execute("SHOW TABLES").fetchall():
    df = duck.execute(f"SELECT * FROM {view}").df()
    df.columns = [slug(c) for c in df.columns]

    # convert *any* date-ish column → ISO string or None
    for col in df.columns:
        if DATE_COL_RE.search(col):
            s = pd.to_datetime(df[col], errors="coerce")
            df[col] = (
                s.dt.strftime("%Y-%m-%d")      # NaT → NaN
                 .where(s.notna(), None)       # NaN → None
            )

    pk = KEY_MAP.get(view)
    db[view].insert_all(
        df.to_dict("records"),
        pk=pk,
        alter=True,
        replace=True,
        batch_size=5000,
    )
    if pk:
        db[view].create_index(pk)

    print(f"✔ {view:<24} {len(df):>8,} rows  pk={pk}")

# ------------------------------------------------------------------ #
# 4 . FTS + compatibility views for the Streamlit UI
# ------------------------------------------------------------------ #
db["pub_water_systems"].enable_fts(["pws_name"], replace=True)

db.conn.executescript("""
    DROP VIEW IF EXISTS water_systems;
    DROP VIEW IF EXISTS violations;
    CREATE VIEW water_systems AS SELECT * FROM pub_water_systems;
    CREATE VIEW violations  AS SELECT * FROM violations_enforcement;
""")

print("\n🎉  georgia_water.db rebuilt – ready for `streamlit run app.py`")