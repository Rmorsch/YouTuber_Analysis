"""YouTube Data API v3 resource.

Uses the quota-cheap endpoint chain instead of `search.list` (100 units
per call, against a 10,000/day quota):

    channels.list       -> 1 unit
    playlistItems.list  -> 1 unit per page (up to 50 items/page)
    videos.list         -> 1 unit per call (up to 50 ids/call)

That chain relies on every channel having an "uploads" playlist that
already lists its videos in upload order, so nothing here needs to
*search* for content -- only look up what a channel ID already points to.
"""

from typing import Any, Iterator

from dagster import ConfigurableResource, InitResourceContext
from googleapiclient.discovery import build
from pydantic import PrivateAttr


class YouTubeAPIResource(ConfigurableResource):
    """Thin wrapper around the YouTube Data API v3 client."""

    api_key: str

    _client: Any = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = build("youtube", "v3", developerKey=self.api_key, cache_discovery=False)

    def get_channel(self, channel_id: str) -> dict:
        """1 unit. Returns snippet + statistics + contentDetails (which
        includes the uploads playlist id)."""
        response = (
            self._client.channels()
            .list(part="snippet,statistics,contentDetails", id=channel_id)
            .execute()
        )
        items = response.get("items", [])
        if not items:
            raise ValueError(f"No channel found for id={channel_id!r}")
        return items[0]

    @staticmethod
    def get_uploads_playlist_id(channel: dict) -> str:
        return channel["contentDetails"]["relatedPlaylists"]["uploads"]

    def iter_playlist_items(
        self, playlist_id: str, page_token: str | None = None
    ) -> Iterator[dict]:
        """1 unit per page. Yields raw playlistItems in playlist order.

        NOTE: verify empirically whether a channel's auto-generated
        "uploads" playlist returns newest-first or oldest-first for your
        test channels before relying on `next(...)` to mean "latest
        video" anywhere -- YouTube doesn't document this and behavior has
        been observed to vary. `new_video_sensor` currently assumes
        newest-first; if that's wrong for a given channel, walk pages to
        the last item (by `playlist_position`) instead.
        """
        while True:
            response = (
                self._client.playlistItems()
                .list(
                    part="snippet,contentDetails",
                    playlistId=playlist_id,
                    maxResults=50,
                    pageToken=page_token,
                )
                .execute()
            )
            yield from response.get("items", [])
            page_token = response.get("nextPageToken")
            if not page_token:
                return

    def get_videos(self, video_ids: list[str]) -> list[dict]:
        """1 unit per call, up to 50 ids per call. Returns snippet +
        statistics + contentDetails for each video."""
        results: list[dict] = []
        for i in range(0, len(video_ids), 50):
            batch = video_ids[i : i + 50]
            response = (
                self._client.videos()
                .list(part="snippet,statistics,contentDetails", id=",".join(batch))
                .execute()
            )
            results.extend(response.get("items", []))
        return results
