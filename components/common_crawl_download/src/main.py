"""A component that downloads common crawl files."""
import logging
from typing import List

import dask.dataframe as dd
from fondant.component import DaskLoadComponent
from trafilatura.settings import use_config
from utils import extract_bytes_from_warc_file_http, extract_html

logger = logging.getLogger(__name__)


class CommonCrawlDownloadComponent(DaskLoadComponent):
    """Component that download common crawl files."""

    def __init__(self, *_, filters: List[str], extract_plain_text: bool):
        # init global trafilatura config for multi-processing
        self.trafilatura_config = use_config()
        self.trafilatura_config.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")
        self.filters = filters
        self.extract_plain_text = extract_plain_text

    def load(self) -> dd.DataFrame:
        """

        Args:
            dataframe: Pandas dataframe.

        Returns:
            Pandas dataframe
        """
        ddf = self.filter_common_crawl_index()

        ddf = self.download_warc_content(ddf)

        if self.extract_plain_text:
            ddf["url_content"] = ddf["url_content"].apply(
                lambda x: extract_html(x, self.trafilatura_config), meta=("url_content", "str"))

        return ddf

    def filter_common_crawl_index(self) -> dd.DataFrame:
        """Use common crawl index and provided filters to retrieve relevant pages
        from common crawl.
        """
        # TODO implementation by @shayorshay
        # final dataframe should contain: url(index), warc_path, warc_offset, warc_length

    def download_warc_content(self, dataframe: dd.DataFrame) -> str:
        return dataframe.assign(content=dataframe.apply(
            lambda row: extract_bytes_from_warc_file_http(row["warc_path"], row["warc_offset"],
                                                          row["warc_length"]),
            axis=1,
            meta=("content", "str")))
