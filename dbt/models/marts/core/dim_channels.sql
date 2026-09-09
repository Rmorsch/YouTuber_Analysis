-- Latest known state per channel, for joining onto fact tables without
-- repeating slowly-changing attributes on every snapshot row.

with ranked as (
    select
        *,
        row_number() over (
            partition by channel_id order by snapshot_date desc
        ) as rn
    from {{ ref('stg_youtube__channels') }}
)

select
    channel_id,
    channel_name,
    uploads_playlist_id
from ranked
where rn = 1
