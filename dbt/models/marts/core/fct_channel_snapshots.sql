{{
    config(
        materialized='incremental',
        unique_key=['channel_id', 'snapshot_date'],
        incremental_strategy='merge',
    )
}}

-- The lag() window needs the full history to compute a correct delta for
-- the oldest new row, so it's computed over all of staging (a cheap
-- view) and only THEN filtered down to the rows this run actually needs
-- to merge -- filtering staging itself first would starve lag() of the
-- prior day's value for the first new snapshot_date.
with channels as (
    select * from {{ ref('stg_youtube__channels') }}
),

with_deltas as (
    select
        channel_id,
        snapshot_date,
        channel_name,
        subscriber_count,
        view_count,
        video_count,
        subscriber_count - lag(subscriber_count) over (
            partition by channel_id order by snapshot_date
        ) as subscriber_count_delta
    from channels
)

select * from with_deltas

{% if is_incremental() %}
where snapshot_date > (select max(snapshot_date) from {{ this }})
{% endif %}
