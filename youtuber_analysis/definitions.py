"""Top-level Dagster Definitions object -- everything Dagster needs to
know about the project is wired together here."""

from dagster import Definitions, EnvVar
from dagster_dbt import DbtCliResource

from .assets import raw_channels, raw_google_trends, raw_playlist_items, raw_videos, youtube_dbt_assets
from .assets.dbt_assets import DBT_PROFILES_DIR, DBT_PROJECT_DIR
from .jobs import ingestion_job
from .resources.snowflake_io_manager import build_snowflake_io_manager
from .resources.trends import GoogleTrendsResource
from .resources.youtube_api import YouTubeAPIResource
from .sensors import new_video_sensor

# Every credential below is an EnvVar, not a plain os.environ[...] read: that
# defers resolution to when a resource is actually used (a materialization,
# a sensor tick), rather than the moment this module is imported. That's
# what lets `dagster dev` load the asset graph even before real credentials
# are in place -- it only breaks when something tries to actually call out.
defs = Definitions(
    assets=[raw_channels, raw_playlist_items, raw_videos, raw_google_trends, youtube_dbt_assets],
    jobs=[ingestion_job],
    sensors=[new_video_sensor],
    resources={
        "youtube_api": YouTubeAPIResource(api_key=EnvVar("YOUTUBE_API_KEY")),
        "google_trends": GoogleTrendsResource(),
        "dbt": DbtCliResource(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR),
        "snowflake_io_manager": build_snowflake_io_manager("RAW"),
    },
)
