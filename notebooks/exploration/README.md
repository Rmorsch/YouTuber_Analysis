# Exploration notebooks

Standalone prototypes written before the Dagster pipeline existed. They
are not imported by the `youtuber_analysis` package; the production
equivalents live in `youtuber_analysis/assets/`.

The top-level notebooks read credentials from a local `config.py` in this
directory (gitignored).

| Notebook | What it explores |
|----------|------------------|
| `youtube_api_prototype.ipynb` | channels -> playlistItems -> videos chain, written to local Postgres. Prototype of `assets/raw_youtube.py`. |
| `youtube_video_analysis.ipynb` | First pass at analyzing `VIDEO_STATISTICS` from Postgres (duration parsing). |
| `socialblade_history.ipynb` | Social Blade API: up to ~3 years of daily historical channel stats, which daily API snapshots can't backfill. |
| `wayback/subcount_scraper_requests.ipynb` | Historical subscriber counts scraped from Wayback Machine snapshots (requests + BeautifulSoup). |
| `wayback/subcount_scraper_selenium.ipynb` | Same idea using Selenium, for JS-rendered snapshots. |
| `wayback/sample_channel_page.html` | Saved channel page used to develop the scraper selectors. |
