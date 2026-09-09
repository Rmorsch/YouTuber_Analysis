"""Secondary destination write -- TODO, not yet wired up.

Design intent: alongside the Snowflake warehouse, mirror raw ingestion
output to a Postgres instance running on the same DigitalOcean droplet as
Dagster. This gives a cheap durability/backup layer that doesn't depend
on Snowflake credit spend and can be queried directly during local
development.

Not required for the pipeline to run end-to-end -- Snowflake is the
system of record. This resource is a placeholder so the intent is
visible in the codebase rather than only in planning notes.

TODO:
  - Provision Postgres on the droplet and add its connection string to
    .env as POSTGRES_CONNECTION_STRING.
  - Implement `write_dataframe` (e.g. via SQLAlchemy + psycopg2,
    `df.to_sql(..., if_exists="append")`).
  - Wire a `PostgresResource` instance into `definitions.py` and call it
    from the raw assets alongside the Snowflake I/O manager write.
"""

import pandas as pd
from dagster import ConfigurableResource


class PostgresResource(ConfigurableResource):
    connection_string: str

    def write_dataframe(self, table: str, df: pd.DataFrame) -> None:
        raise NotImplementedError(
            "Secondary Postgres write is not implemented yet -- see module docstring."
        )
