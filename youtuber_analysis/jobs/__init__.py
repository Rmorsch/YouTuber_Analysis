from dagster import define_asset_job

from ..assets import raw_channels, raw_playlist_items, raw_videos, youtube_dbt_assets

ingestion_job = define_asset_job(
    name="ingestion_job",
    description="Pull the daily channel/video snapshot from YouTube and rebuild dbt models.",
    selection=[raw_channels, raw_playlist_items, raw_videos, youtube_dbt_assets],
)
