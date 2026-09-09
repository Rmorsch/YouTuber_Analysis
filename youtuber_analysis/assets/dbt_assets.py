"""dbt asset wiring: dagster-dbt loads the dbt project's manifest and
exposes every dbt model as a Dagster asset, keeping the orchestrator's
and the transformation layer's lineage graphs in sync automatically
instead of maintaining a second, hand-written dependency graph."""

from pathlib import Path

from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR / "profiles"

dbt_project = DbtProject(project_dir=DBT_PROJECT_DIR, profiles_dir=DBT_PROFILES_DIR)
dbt_project.prepare_if_dev()


class RawAssetDbtTranslator(DagsterDbtTranslator):
    """Maps each dbt source (declared in _youtube__sources.yml, e.g.
    raw.raw_channels) onto the SAME asset key as the Python asset that
    actually produces it (e.g. raw_channels in assets/raw_youtube.py).

    Without this, dagster-dbt's default naming (AssetKey(["raw",
    "raw_channels"])) doesn't match our Python asset's key
    (AssetKey(["raw_channels"])), and the two show up as disconnected
    nodes in the asset graph instead of one continuous
    ingestion -> staging -> marts lineage."""

    def get_asset_key(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "source":
            return AssetKey(dbt_resource_props["name"])
        return super().get_asset_key(dbt_resource_props)


@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=RawAssetDbtTranslator())
def youtube_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
