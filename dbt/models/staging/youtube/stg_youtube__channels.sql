-- Thin rename/cast layer over the raw channel snapshot. Materialized as
-- a view (see dbt_project.yml) since RAW already holds the history and
-- this adds no meaningful compute.

with source as (
    select * from {{ source('raw', 'raw_channels') }}
)

select
    snapshot_date::date        as snapshot_date,
    channel_id,
    channel_name,
    uploads_playlist_id,
    subscriber_count::number   as subscriber_count,
    view_count::number         as view_count,
    video_count::number        as video_count
from source
