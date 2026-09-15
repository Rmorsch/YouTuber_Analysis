# Exploration notebooks

Standalone prototypes written before the Dagster pipeline existed. They
are not imported by the `youtuber_analysis` package; the production
equivalents live in `youtuber_analysis/assets/`.

The top-level notebooks read credentials from a local `config.py` in this
directory (gitignored), except `youtube_api_prototype.ipynb`, which reads the
repo-root `.env`.

| Notebook | What it explores |
|----------|------------------|
| `youtube_api_prototype.ipynb` | Stage 1 initial load: every video for every channel in `config/channels.yaml` into a single Snowflake table, `RAW.YOUTUBE_VIDEOS` (one row per video per snapshot date). Imports `load_channel_config` from the package; otherwise standalone. |
| `youtube_video_analysis.ipynb` | First pass at analyzing `VIDEO_STATISTICS` from Postgres (duration parsing). |
| `socialblade_history.ipynb` | Social Blade API: up to ~3 years of daily historical channel stats, which daily API snapshots can't backfill. |
| `wayback/subcount_scraper_requests.ipynb` | Historical subscriber counts scraped from Wayback Machine snapshots (requests + BeautifulSoup). |
| `wayback/subcount_scraper_selenium.ipynb` | Same idea using Selenium, for JS-rendered snapshots. |
| `wayback/sample_channel_page.html` | Saved channel page used to develop the scraper selectors. |
