-- One row per product (grain = product_id), including products with no reviews.
-- Reviews and paid sales are aggregated SEPARATELY to product level first and only then
-- joined to the product table. Joining orders directly to reviews would repeat each order's
-- amount once per review of the same product and inflate revenue.
--
-- Batch revenue = orders with status 'paid' only; fct_orders.amount already includes quantity.
-- Negative review = rating <= 2. A product with no reviews keeps review_count = 0, avg_rating = null.

with review_stats as (
    select
        product_id,
        count(*)                                  as review_count,
        round(avg(rating), 2)                     as avg_rating,
        count(*) filter (where rating <= 2)       as negative_reviews
    from {{ ref('stg_reviews') }}
    group by product_id
),

sales_stats as (
    select
        product_id,
        count(*)                                  as paid_orders,
        sum(amount)                               as revenue
    from {{ ref('fct_orders') }}
    where status = 'paid'
    group by product_id
)

select
    p.product_id,
    p.product_name,
    p.category,
    coalesce(r.review_count, 0)       as review_count,
    r.avg_rating                      as avg_rating,          -- stays null when there are no reviews
    coalesce(r.negative_reviews, 0)   as negative_reviews,
    coalesce(s.paid_orders, 0)        as paid_orders,
    coalesce(s.revenue, 0)            as revenue
from {{ ref('stg_products') }} p
left join review_stats r using (product_id)
left join sales_stats  s using (product_id)
