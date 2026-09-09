"""Raw Google Trends ingestion (secondary/enrichment source)."""

import pandas as pd
from dagster import AssetExecutionContext, asset

from ..resources.trends import GoogleTrendsResource
from .raw_youtube import load_channel_config


@asset(group_name="raw_trends", io_manager_key="snowflake_io_manager", compute_kind="python")
def raw_google_trends(
    context: AssetExecutionContext, google_trends: GoogleTrendsResource
) -> pd.DataFrame:
    """Interest-over-time snapshot for each configured channel's search
    keyword. pytrends caps requests at 5 keywords, so channels missing a
    `trends_keyword` are skipped and the rest are chunked."""
    channels = load_channel_config()
    keywords = [c["trends_keyword"] for c in channels if c.get("trends_keyword")]

    frames = []
    for i in range(0, len(keywords), 5):
        chunk = keywords[i : i + 5]
        df = google_trends.interest_over_time(chunk)
        if not df.empty:
            frames.append(df.reset_index())

    context.add_output_metadata({"num_keywords": len(keywords)})
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)
