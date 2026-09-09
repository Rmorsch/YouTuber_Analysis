"""Snowflake I/O manager: pandas DataFrames returned from an asset are
written straight to a Snowflake table named `<schema>.<asset_name>`. RAW
assets land in the RAW schema (append-only snapshots); dbt takes it from
there into STAGING (views) and MARTS (tables)."""

from dagster import EnvVar
from dagster_snowflake_pandas import SnowflakePandasIOManager


def build_snowflake_io_manager(schema: str = "RAW") -> SnowflakePandasIOManager:
    # EnvVar defers the actual env var read to when the resource is used,
    # not to when this function runs (which is at Definitions-load time) --
    # see the comment in definitions.py.
    return SnowflakePandasIOManager(
        account=EnvVar("SNOWFLAKE_ACCOUNT"),
        user=EnvVar("SNOWFLAKE_USER"),
        password=EnvVar("SNOWFLAKE_PASSWORD"),
        role=EnvVar("SNOWFLAKE_ROLE"),
        warehouse=EnvVar("SNOWFLAKE_WAREHOUSE"),
        database=EnvVar("SNOWFLAKE_DATABASE"),
        schema=schema,
    )
