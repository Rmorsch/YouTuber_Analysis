"""Raw-layer ingestion assets: channels -> playlistItems -> videos.

Follows the quota-cheap endpoint chain (see resources/youtube_api.py).
Raw assets land as close to the API response shape as practical, keyed so
dbt staging models can do minimal, mostly-renaming transformations. Each
of these is an append-only daily snapshot -- there is no update-in-place,
because a day that wasn't captured can never be recovered.
"""

import datetime as dt
import pathlib

import pandas as pd
import yaml
from dagster import AssetExecutionContext, asset

from ..resources.youtube_api import YouTubeAPIResource

CONFIG_DIR = pathlib.Path(__file__).resolve().parents[1] / "config"
CHANNELS_CONFIG_PATH = CONFIG_DIR / "channels.yaml"


def load_channel_config() -> list[dict]:
    with CHANNELS_CONFIG_PATH.open() as f:
        config = yaml.safe_load(f)
    return config["channels"]


@asset(group_name="raw_youtube", io_manager_key="snowflake_io_manager", compute_kind="python")
def raw_channels(context: AssetExecutionContext, youtube_api: YouTubeAPIResource) -> pd.DataFrame:
    """Daily snapshot of channel-level stats (subscribers, views, video
    count) for every configured channel."""
    channels = load_channel_config()
    snapshot_date = dt.date.today().isoformat()
    rows = []
    for channel in channels:
        raw = youtube_api.get_channel(channel["channel_id"])
        rows.append(
            {
                "snapshot_date": snapshot_date,
                "channel_id": channel["channel_id"],
                "channel_name": raw["snippet"]["title"],
                "uploads_playlist_id": youtube_api.get_uploads_playlist_id(raw),
                "subscriber_count": int(raw["statistics"].get("subscriberCount", 0)),
                "view_count": int(raw["statistics"].get("viewCount", 0)),
                "video_count": int(raw["statistics"].get("videoCount", 0)),
            }
        )
    context.add_output_metadata({"num_channels": len(rows)})
    return pd.DataFrame(rows)


@asset(group_name="raw_youtube", io_manager_key="snowflake_io_manager", compute_kind="python")
def raw_playlist_items(
    context: AssetExecutionContext,
    youtube_api: YouTubeAPIResource,
    raw_channels: pd.DataFrame,
) -> pd.DataFrame:
    """One row per video ever seen in each channel's uploads playlist.
    This full listing is also what the new-video sensor's cursor logic
    diffs against, just cheaper (only the newest page)."""
    rows = []
    for _, channel_row in raw_channels.iterrows():
        for item in youtube_api.iter_playlist_items(channel_row["uploads_playlist_id"]):
            rows.append(
                {
                    "channel_id": channel_row["channel_id"],
                    "video_id": item["contentDetails"]["videoId"],
                    "published_at": item["contentDetails"].get("videoPublishedAt"),
                    "playlist_position": item["snippet"].get("position"),
                }
            )
    context.add_output_metadata({"num_videos_seen": len(rows)})
    return pd.DataFrame(rows)


@asset(group_name="raw_youtube", io_manager_key="snowflake_io_manager", compute_kind="python")
def raw_videos(
    context: AssetExecutionContext,
    youtube_api: YouTubeAPIResource,
    raw_playlist_items: pd.DataFrame,
) -> pd.DataFrame:
    """Daily snapshot of per-video stats (views, likes, comments) for
    every video seen so far across all configured channels."""
    video_ids = raw_playlist_items["video_id"].unique().tolist()
    snapshot_date = dt.date.today().isoformat()
    videos = youtube_api.get_videos(video_ids)
    rows = [
        {
            "snapshot_date": snapshot_date,
            "video_id": video["id"],
            "channel_id": video["snippet"]["channelId"],
            "title": video["snippet"]["title"],
            "published_at": video["snippet"]["publishedAt"],
            "duration": video["contentDetails"]["duration"],
            "view_count": int(video["statistics"].get("viewCount", 0)),
            "like_count": int(video["statistics"].get("likeCount", 0)),
            "comment_count": int(video["statistics"].get("commentCount", 0)),
        }
        for video in videos
    ]
    context.add_output_metadata({"num_videos": len(rows)})
    return pd.DataFrame(rows)
