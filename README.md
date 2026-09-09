# YouTuber Analysis

A data pipeline that treats YouTube creators' public stats as a real
analytics problem: daily channel/video snapshots ingested from the
YouTube Data API v3 (plus Google Trends as a secondary signal), landed in
Snowflake, and modeled with dbt into creator-growth marts.

## Stack

| Layer          | Tool                                  |
|----------------|----------------------------------------|
| Orchestration  | Dagster (assets, sensors, jobs)        |
| Transformation | dbt                                    |
| Warehouse      | Snowflake                              |
| Hosting        | Self-hosted DigitalOcean droplet       |
| Sources        | YouTube Data API v3, Google Trends (`pytrends`) |

## Architecture

```
YouTube Data API v3          Google Trends
  channels.list                 pytrends
  playlistItems.list                |
  videos.list                       |
        |                           |
        v                           v
   raw_channels    raw_playlist_items    raw_videos    raw_google_trends
   \_______________________  Snowflake RAW  ________________________/
                              |  dbt  |
                    STAGING (views, thin rename/cast)
                              |
                    MARTS (tables, incremental merge)
                    dim_channels, fct_channel_snapshots,
                    fct_video_snapshots
```

Ingestion follows the quota-cheap endpoint chain -- `channels.list` ->
`playlistItems.list` -> `videos.list`, ~1 unit per call -- instead of the
100-unit `search.list` endpoint, against a 10,000 unit/day quota. A
cursor-based sensor (`new_video_sensor`) checks each channel's uploads
playlist and triggers an ingestion run only when it sees a new video.

Snapshots are **append-only and non-retroactive**: a day that isn't
captured can't be backfilled. That's why ingestion is entirely
config-driven (`youtuber_analysis/config/channels.yaml`) and channel-agnostic
from day one -- the channel list should be seeded wide immediately so
history accumulates, even though analysis itself proceeds in two phases:

- **Phase 1** -- deep-dive analysis on a handful of channels.
- **Phase 2** -- generalize to cross-creator growth analysis once the
  dataset has enough history.

## Setup

```bash
cp .env.example .env        # fill in YouTube API key + Snowflake creds
pip install -e ".[dev]"
cd dbt && dbt deps && cd -
cp dbt/profiles/profiles.yml.example ~/.dbt/profiles.yml   # fill in creds
```

Then, add real channels to `youtuber_analysis/config/channels.yaml`
(replace the `UC_PLACEHOLDER_*` entries), and run:

```bash
make dev        # dagster dev, at localhost:3000
make dbt-run    # dbt build directly, without Dagster
make test       # pytest
```

## Open TODOs

- [ ] Replace placeholder channel IDs in `youtuber_analysis/config/channels.yaml`
      with real ones.
- [ ] Wire up the secondary destination write to Postgres on the droplet
      (`youtuber_analysis/resources/postgres.py` is a stub).
- [ ] Verify whether a channel's uploads playlist is newest-first or
      oldest-first (undocumented; see caveat in
      `youtuber_analysis/resources/youtube_api.py`) before trusting
      `new_video_sensor` in production.
