with source as (
    select * from {{ source('raw', 'raw_playlist_items') }}
)

select
    channel_id,
    video_id,
    published_at::timestamp_ntz as published_at,
    playlist_position::number   as playlist_position
from source
