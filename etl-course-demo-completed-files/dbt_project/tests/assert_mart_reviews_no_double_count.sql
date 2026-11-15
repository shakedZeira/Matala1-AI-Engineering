-- Guards mart_product_reviews against the classic fan-out bug: joining orders directly to reviews
-- repeats every order's amount once per review of that product and inflates revenue.
-- The test returns rows (= fails) if the mart disagrees with its sources:
--   * total revenue / paid_orders must equal paid revenue / count in fct_orders
--   * total review_count must equal the clean review count in stg_reviews
--   * revenue per product must equal the per-product paid revenue computed straight from fct_orders
with mart as (
    select sum(revenue) as revenue, sum(paid_orders) as paid_orders, sum(review_count) as review_count
    from {{ ref('mart_product_reviews') }}
),
src as (
    select
        (select coalesce(sum(amount), 0) from {{ ref('fct_orders') }} where status = 'paid') as revenue,
        (select count(*) from {{ ref('fct_orders') }} where status = 'paid')                as paid_orders,
        (select count(*) from {{ ref('stg_reviews') }})                                      as review_count
),
totals_mismatch as (
    select 'totals' as problem, m.revenue as mart_value, s.revenue as source_value
    from mart m cross join src s
    where m.revenue <> s.revenue or m.paid_orders <> s.paid_orders or m.review_count <> s.review_count
),
per_product_mismatch as (
    select 'product ' || m.product_id as problem, m.revenue as mart_value, coalesce(f.revenue, 0) as source_value
    from {{ ref('mart_product_reviews') }} m
    left join (
        select product_id, sum(amount) as revenue
        from {{ ref('fct_orders') }} where status = 'paid' group by product_id
    ) f using (product_id)
    where m.revenue <> coalesce(f.revenue, 0)
)
select * from totals_mismatch
union all
select * from per_product_mismatch
