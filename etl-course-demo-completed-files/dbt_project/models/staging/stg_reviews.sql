-- Review cleaning (exercise step 2). One row per valid review.
-- Business rules, applied in this order so the row counts can be reported after each rule:
--   1. rating must be an integer 1..5 (the source has 0, 7 and null)
--   2. text must be non-empty after trim
--   3. duplicate review_id -> keep the EARLIEST VALID row by created_at
--      (dedup runs after rules 1-2, so an invalid early copy never wins)
--
-- Run:   cd dbt_project && dbt build --select stg_reviews --profiles-dir .
-- Check: python exercises/check_exercise.py 2

with source as (
    select * from {{ source('raw', 'reviews') }}
),

typed as (
    select
        cast(review_id as integer)               as review_id,
        cast(order_id as integer)                as order_id,
        cast(product__id as integer)             as product_id,   -- dlt flattens product.id -> product__id
        try_cast(rating as integer)              as rating,
        nullif(trim(cast(text as varchar)), '')  as review_text,
        -- dlt loads created_at as TIMESTAMP WITH TIME ZONE (UTC). A plain cast to timestamp would
        -- shift it into the machine's session time zone (+2/+3h in Israel), making the output
        -- machine-dependent. timezone('UTC', ...) returns the source wall-clock value everywhere.
        timezone('UTC', cast(created_at as timestamptz)) as created_at
    from source
),

valid_rating as (
    select * from typed
    where rating between 1 and 5            -- null fails the comparison, so null ratings are dropped too
),

valid_text as (
    select * from valid_rating
    where review_text is not null
),

deduped as (
    select * from valid_text
    qualify row_number() over (
        partition by review_id
        order by created_at asc, order_id asc   -- order_id only breaks exact created_at ties deterministically
    ) = 1
)

select * from deduped
