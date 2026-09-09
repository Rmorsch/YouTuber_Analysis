"""Google Trends resource via pytrends -- a secondary/enrichment source
that complements the YouTube Data API's creator-side stats with a signal
for audience-side search interest."""

from typing import Any

import pandas as pd
from dagster import ConfigurableResource, InitResourceContext
from pydantic import PrivateAttr
from pytrends.request import TrendReq


class GoogleTrendsResource(ConfigurableResource):
    hl: str = "en-US"
    tz: int = 0

    _client: Any = PrivateAttr(default=None)

    def setup_for_execution(self, context: InitResourceContext) -> None:
        self._client = TrendReq(hl=self.hl, tz=self.tz)

    def interest_over_time(self, keywords: list[str], timeframe: str = "today 3-m") -> pd.DataFrame:
        """pytrends caps requests at 5 keywords -- callers are expected to
        chunk their keyword list before calling this."""
        if len(keywords) > 5:
            raise ValueError("pytrends supports at most 5 keywords per request")
        self._client.build_payload(kw_list=keywords, timeframe=timeframe)
        return self._client.interest_over_time()
