"""
תרגיל כיתה - שלב 1: Extract & Load של ביקורות לקוחות עם dlt
=============================================================
המשימה: לטעון את data/raw/reviews.json לסכמה raw ב-warehouse.duckdb, לטבלה בשם reviews.

רמזים:
  * הסתכלו איך customers() בנוי ב-pipelines/02_dlt_pipeline.py
  * הקובץ הוא snapshot מלא - איזה write_disposition מתאים?
  * אל תנקו כאן כלום! כפילויות ולכלוך נשארים ב-raw. ניקוי = dbt.

הרצה:     python pipelines/04_reviews_pipeline.py
בדיקה:    python exercises/check_exercise.py 1
"""
import json
from pathlib import Path

import dlt

ROOT = Path(__file__).resolve().parent.parent
RAW = ROOT / "data" / "raw"
WAREHOUSE = ROOT / "warehouse.duckdb"


# Full snapshot export: every run replaces the table, so re-running can never duplicate rows.
# (Orders use merge because they are incremental by updated_at; this file is not.)
# No cleaning here on purpose: duplicates, invalid ratings and empty texts stay in raw.
@dlt.resource(name="reviews", write_disposition="replace")
def reviews():
    with open(RAW / "reviews.json", encoding="utf-8") as f:
        yield json.load(f)


def make_pipeline():
    return dlt.pipeline(
        pipeline_name="shop_il_reviews",
        pipelines_dir=str(ROOT / ".dlt" / "pipelines"),
        destination=dlt.destinations.duckdb(str(WAREHOUSE)),
        dataset_name="raw",
    )


def main() -> None:
    pipeline = make_pipeline()
    load_info = pipeline.run(reviews())
    print(load_info)

    # Which tables did dlt create? (nested product.id and the tags list are normalized)
    with pipeline.sql_client() as client:
        tables = client.execute_sql(
            "select table_name from information_schema.tables "
            "where table_schema = 'raw' and table_name like 'reviews%' order by 1")
        print("\nreview tables in raw:")
        for (t,) in tables:
            n = client.execute_sql(f'select count(*) from raw."{t}"')[0][0]
            print(f"  {t:<20} {n:>6} rows")


if __name__ == "__main__":
    main()
