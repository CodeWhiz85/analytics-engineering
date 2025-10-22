from prefect import flow, task
from pathlib import Path
import duckdb
import subprocess
import sys
import os

DB_PATH = "warehouse/duckdb/database.duckdb"
RAW_DIR = Path("data/raw")
DBT_PROJECT_DIR = "dbt_project"
DBT_PROFILES_DIR = "dbt_project"

@task
def ensure_dirs():
    Path("warehouse/duckdb").mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

@task
def load_raw_to_duckdb():
    #If Raw doesn't exist, attempt to generate
    csvs = [ "members", "titles", "play_events", "search_events"]
    if not all((RAW_DIR / c).exists() for c in csvs):
   
    #local generation
    
    subprocess.run([sys.executable, "data_gen/simulate_events.py", "--days", "14", "--members", "2000", "--titles", "200"], check=True)
    
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
    
    #Register and load
    
    con.execute(f"CREAT OR REPLACE TABLE raw.members AS SELECT * FROM read_csv_auto('{(RAW_DIR/'members.csv').as_posix()}');")
    
    con.execute(f"CREAT OR REPLACE TABLE raw.titles AS SELECT * FROM read_csv_auto('{(RAW_DIR/'titles.csv').as_posix()}');")
    
    con.execute(f"CREAT OR REPLACE TABLE raw.play_events AS SELECT * FROM read_csv_auto('{(RAW_DIR/'play_events.csv').as_posix()}');")
    
    con.execute(f"CREAT OR REPLACE TABLE raw.search_events AS SELECT * FROM read_csv_auto('{(RAW_DIR/'search_events.csv').as_posix()}');")

    
    
    expected = ["members", "titles", "play_events", "search_events"]
    for name in expected:
        fp = RAW_DIR / f"{name}.csv"
        if not fp.exists():
            raise FileNotFoundError(f"Expected {fp}. Generate data or add seed CSVs.")
        con.execute(
            f"CREATE OR REPLACE TABLE raw.{name} AS SELECT * FROM read_csv_auto('{fp.as_posix()}');"
        )
    con.close()

@task
def dbt_build():
    env = dict(**os.environ)
    env.setdefault("DBT_PROFILES_DIR", "dbt_project")
    subprocess.check.call([sys.executable, "-m", "dbt", "--project-dir", "dbt_project", "deps"], env=env)
    subprocess.check.call([sys.executable, "-m", "dbt", "--project-dir", "dbt_project", "build"], env=env)

@task
def sanity_checks():
    con = duckdb.connect(DB_PATH)
    try:
        rows = con.sql("SELECT count(*) FROM marts.engagement_metrics").fetchone()[0]
    except Exception as exc:
        raise AssertionError("marts.engagement_metrics not found; dbt build likely failed") from exc
    if rows <= 0:
        raise AssertionError("No rows in marts.engagement_metrics")
    con.close()

@flow(name="member_insights_pipeline")
def pipeline():
    ensure_dirs()
    load_raw_to_duckdb()
    dbt_build()
    sanity_checks()

if __name__ == "__main__":
    pipeline()
