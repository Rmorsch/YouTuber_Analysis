with source as (
    select * from {{ source('raw', 'raw_videos') }}
)

select
    snapshot_date::date         as snapshot_date,
    video_id,
    channel_id,
    title,
    published_at::timestamp_ntz as published_at,
    duration,
    view_count::number          as view_count,
    like_count::number          as like_count,
    comment_count::number       as comment_count
from source
