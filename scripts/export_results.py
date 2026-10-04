"""
Run every query in sql/results_snapshot.sql against the Databricks SQL warehouse
and save each result as a CSV in results/.
"""

import csv
import os
import re
import sys
from pathlib import Path

from databricks import sql

HOST = os.environ.get("DATABRICKS_HOST", "dbc-0d9c6991-c514.cloud.databricks.com")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/159364c22c7e4fd8")
TOKEN = os.environ.get("DATABRICKS_TOKEN")

ROOT = Path(__file__).resolve().parents[1]
SQL_FILE = ROOT / "sql" / "results_snapshot.sql"
OUT_DIR = ROOT / "results"

# Each query starts with a header like:  -- Q1. Title  ->  results/name.csv
HEADER = re.compile(r"^-- Q(\d+)\..*?->\s*(results/\S+\.csv)\s*$", re.MULTILINE)


def load_queries(path):
    text = path.read_text(encoding="utf-8-sig")
    headers = list(HEADER.finditer(text))
    queries = []
    for i, h in enumerate(headers):
        end = headers[i + 1].start() if i + 1 < len(headers) else len(text)
        body = text[h.end():end]
        body = "\n".join(line for line in body.splitlines() if not line.strip().startswith("--"))
        queries.append((f"Q{h.group(1)}", h.group(2), body.strip().rstrip(";").strip()))
    return queries


def main():
    if not TOKEN:
        sys.exit("DATABRICKS_TOKEN is not set.")

    queries = load_queries(SQL_FILE)
    if not queries:
        sys.exit(f"No queries found in {SQL_FILE.name}. Each query needs a '-- Qn. ... -> results/name.csv' header.")
    OUT_DIR.mkdir(exist_ok=True)
    print(f"Running {len(queries)} queries from {SQL_FILE.name}\n")

    with sql.connect(server_hostname=HOST, http_path=HTTP_PATH, access_token=TOKEN) as conn:
        with conn.cursor() as cur:
            for label, out_rel, query in queries:
                cur.execute(query)
                columns = [d[0] for d in cur.description]
                rows = cur.fetchall()
                out_path = ROOT / out_rel
                with out_path.open("w", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerow(columns)
                    writer.writerows([list(r) for r in rows])
                print(f"{label}: {len(rows):>3} rows -> {out_rel}")
                for r in rows[:3]:
                    print("     ", dict(zip(columns, r)))
                print()

    print(f"Done. CSV files saved in {OUT_DIR}")


if __name__ == "__main__":
    main()
