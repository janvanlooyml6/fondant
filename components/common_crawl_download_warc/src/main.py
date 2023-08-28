"""A component that downloads common crawl files."""
import asyncio
import io
import logging
import typing as t

import dask
import httpx
import pandas as pd
from fondant.component import PandasTransformComponent
from utils import (
    extract_html,
    read_warc_content,
)

logger = logging.getLogger(__name__)

dask.config.set(scheduler="processes")

CC_BASE_URL = "http://data.commoncrawl.org"


class CommonCrawlDownloadComponent(PandasTransformComponent):
    """Component that download common crawl files."""

    def __init__(
        self,
        *_,
        extract_plain_text: bool,
        n_records_to_download: t.Optional[int] = None,
    ):
        self.extract_plain_text = extract_plain_text
        self.n_records_to_download = n_records_to_download

    async def download_warc_content(
        self,
        row: t.Any,
        client: httpx.AsyncClient,
    ) -> t.Tuple[str, str, t.Optional[str]]:
        """
        Download content of a single web page.

        Args:
            row: This should be a NamedTuple returned by df.itertuples(), but cannot be
                 typehinted as such.
            client: Httpx client to use

        Returns:
            A tuple containing the index, url, and extracted content
        """
        url = f"{CC_BASE_URL}/{row.warc_filename}"
        headers = {
            "Range": f"bytes={row.warc_record_offset}-"
                     f"{row.warc_record_offset + row.warc_record_length - 1}",
        }

        try:
            response = await client.get(url, headers=headers)
        except Exception as e:
            logger.warning(f"Error downloading {url} with headers {headers}: {repr(e)}")
            return row.Index, url, None
        else:
            with io.BytesIO(response.content) as warc_stream:
                content = read_warc_content(warc_stream)

            if self.extract_plain_text and content is not None:
                content = extract_html(content)

            return row.Index, row.url, content

    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Concurrently download and extract the WARC files referenced in the provided dataframe."""
        html_content = []

        async def download_dataframe() -> None:
            transport = httpx.AsyncHTTPTransport(retries=1)
            async with httpx.AsyncClient(transport=transport, timeout=10) as client:
                html = await asyncio.gather(
                    *[self.download_warc_content(row, client=client)
                      for row in dataframe["webpage"].itertuples()],
                )
                html_content.extend(html)

        asyncio.run(download_dataframe())

        columns = ["url_surtkey", "url", "content"]
        if html_content:
            content_df = pd.DataFrame(html_content, columns=columns)
        else:
            content_df = pd.DataFrame(columns=columns)

        content_df = content_df.dropna()
        content_df = content_df.set_index("url_surtkey", drop=True)

        content_df.columns = [
            ("webpage", "url"),
            ("webpage", "content"),
        ]

        return content_df
