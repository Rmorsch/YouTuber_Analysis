{{
    config(
        materialized='incremental',
        unique_key=['video_id', 'snapshot_date'],
        incremental_strategy='merge',
    )
}}

select
    video_id,
    channel_id,
    snapshot_date,
    title,
    published_at,
    duration,
    view_count,
    like_count,
    comment_count
from {{ ref('stg_youtube__videos') }}

{% if is_incremental() %}
where snapshot_date > (select max(snapshot_date) from {{ this }})
{% endif %}
