"""Cursor-based sensor: fires the ingestion job when any configured
channel's uploads playlist has advanced past the last video id the
sensor saw for it.

The cursor is a JSON blob of {channel_id: last_seen_video_id}, so each
tick costs only 2 quota units per channel (one `channels.list` to get the
uploads playlist id, one `playlistItems.list` page to read its newest
item) rather than diffing full snapshots.

CAVEAT (see resources/youtube_api.py): this assumes the first item
`iter_playlist_items` yields is the most recently uploaded video. Verify
that against real channels before trusting this in production -- if a
channel's uploads playlist turns out to be oldest-first, this needs to
walk to the last page instead of reading the first item.
"""

import json

from dagster import RunRequest, SensorEvaluationContext, sensor

from ..assets.raw_youtube import load_channel_config
from ..jobs import ingestion_job
from ..resources.youtube_api import YouTubeAPIResource


@sensor(job=ingestion_job, minimum_interval_seconds=60 * 30)
def new_video_sensor(context: SensorEvaluationContext, youtube_api: YouTubeAPIResource):
    cursor: dict[str, str] = json.loads(context.cursor) if context.cursor else {}
    new_cursor = dict(cursor)
    found_new_video = False

    for channel in load_channel_config():
        channel_id = channel["channel_id"]
        channel_raw = youtube_api.get_channel(channel_id)
        uploads_playlist_id = youtube_api.get_uploads_playlist_id(channel_raw)

        newest_item = next(youtube_api.iter_playlist_items(uploads_playlist_id), None)
        if newest_item is None:
            continue

        latest_video_id = newest_item["contentDetails"]["videoId"]
        if cursor.get(channel_id) != latest_video_id:
            found_new_video = True
        new_cursor[channel_id] = latest_video_id

    context.update_cursor(json.dumps(new_cursor))

    if found_new_video:
        yield RunRequest(run_key=None)
