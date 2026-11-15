"""Part A verification report: reference values from the assignment, computed independently.

Usage: python partA_verify.py <path to warehouse.duckdb> <path to reviews.json>
The expected-ID check recomputes the cleaning rules in plain Python from the raw JSON,
so it does not depend on the dbt model it is checking.
"""
import json
import sys

import duckdb

wh, src = sys.argv[1], sys.argv[2]
con = duckdb.connect(wh, read_only=True)
q = lambda s: con.execute(s).fetchall()
ok = True


def check(name, got, expected):
    global ok
    good = got == expected
    ok &= good
    print(f"[{'PASS' if good else 'FAIL'}] {name}: got={got} expected={expected}")


print("== Orders (assignment reference values)")
check("raw.orders rows", q("select count(*) from raw.orders")[0][0], 600)
check("staging.stg_orders rows", q("select count(*) from staging.stg_orders")[0][0], 600)
check("marts.fct_orders rows", q("select count(*) from marts.fct_orders")[0][0], 549)
paid = q("select count(*), round(sum(amount),2) from marts.fct_orders where status='paid'")[0]
check("paid orders", paid[0], 402)
check("paid revenue (ILS)", float(paid[1]), 97210.10)

print("== Reviews: counts after each rule")
raw = q("select count(*) from raw.reviews")[0][0]
after_rating = q("select count(*) from raw.reviews where try_cast(rating as integer) between 1 and 5")[0][0]
after_text = q("select count(*) from raw.reviews where try_cast(rating as integer) between 1 and 5 "
               "and nullif(trim(cast(text as varchar)),'') is not null")[0][0]
final = q("select count(*) from staging.stg_reviews")[0][0]
check("raw", raw, 310)
check("after rating rule", after_rating, 304)
check("after text rule", after_text, 298)
check("after duplicate rule (stg_reviews)", final, 289)

print("== Reviews: surviving identifiers (independent recomputation from reviews.json)")
rows = json.load(open(src, encoding="utf-8"))
valid = [r for r in rows
         if isinstance(r.get("rating"), int) and 1 <= r["rating"] <= 5
         and (r.get("text") or "").strip()]
best = {}
for r in valid:  # earliest valid created_at per review_id
    cur = best.get(r["review_id"])
    if cur is None or r["created_at"] < cur["created_at"]:
        best[r["review_id"]] = r
expected_ids = sorted(best)
got_ids = sorted(x[0] for x in q("select review_id from staging.stg_reviews"))
check("surviving review_id set equals expected", got_ids == expected_ids, True)
check("count of surviving ids", len(got_ids), 289)
chosen_ok = all(
    str(q(f"select created_at from staging.stg_reviews where review_id={i}")[0][0]).replace(" ", "T")[:19]
    == best[i]["created_at"][:19] for i in expected_ids)
check("kept row is the earliest valid created_at", chosen_ok, True)

print("== Mart: one row per product")
check("mart rows", q("select count(*) from marts.mart_product_reviews")[0][0], 8)
check("distinct product_id", q("select count(distinct product_id) from marts.mart_product_reviews")[0][0], 8)
check("sum of mart revenue == paid fct revenue",
      float(q("select round(sum(revenue),2) from marts.mart_product_reviews")[0][0]), 97210.10)
check("sum of mart paid_orders", q("select sum(paid_orders) from marts.mart_product_reviews")[0][0], 402)
check("sum of mart review_count", q("select sum(review_count) from marts.mart_product_reviews")[0][0], 289)
print("\nproduct_id | name | reviews | avg | negative | paid_orders | revenue")
for r in q("select product_id, product_name, review_count, avg_rating, negative_reviews, paid_orders, revenue "
           "from marts.mart_product_reviews order by product_id"):
    print(" | ".join(str(x) for x in r))
nr = q("select count(*) from marts.mart_product_reviews where review_count=0")[0][0]
print(f"products with zero reviews: {nr}")

print("\nOVERALL:", "ALL PASS" if ok else "SOME FAILED")
sys.exit(0 if ok else 1)
