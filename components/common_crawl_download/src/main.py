"""A component that downloads common crawl files."""
import logging
from typing import List, Optional

import dask.dataframe as dd
import pyarrow.dataset as ds
from fondant.component import DaskLoadComponent
from trafilatura.settings import use_config
from utils import (
    extract_bytes_from_warc_file_http,
    extract_html,
    validate_commoncrawl_index_filters,
)

logger = logging.getLogger(__name__)

CC_INDEX_PATH = "s3://commoncrawl/cc-index/table/cc-main/warc/"
CC_BASE_URL = "s3://"

class CommonCrawlDownloadComponent(DaskLoadComponent):
    """Component that download common crawl files."""

    def __init__(self,
                 *_,
                 filters: List,
                 extract_plain_text: bool,
                 n_records_to_download: Optional[int] = None):
        # init global trafilatura config for multi-processing
        self.trafilatura_config = use_config()
        self.trafilatura_config.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")
        self.filters = validate_commoncrawl_index_filters(filters)
        self.extract_plain_text = extract_plain_text
        self.n_records_to_download = n_records_to_download


    def filter_common_crawl_index(self) -> dd.DataFrame:
        """Use common crawl index and provided filters to retrieve relevant pages
        from common crawl.
        """
        logger.info("Filtering common crawl index...")
        output_columns = [
            "url_surtkey",
            "url",
            "warc_filename",
            "warc_record_offset",
            "warc_record_length",
        ]

        cc_index = ds.dataset(CC_INDEX_PATH, format="parquet", partitioning="hive")
        warc_files = cc_index.files
        warc_files = [CC_BASE_URL + file for file in warc_files] # TODO: change to http
        warc_ddf = dd.read_parquet(
            warc_files,
            engine='pyarrow',
            filters=self.filters,
            columns=output_columns,
        )

        if self.n_records_to_download is not None:
            warc_ddf = warc_ddf.head(self.n_records_to_download)

        return warc_ddf


    def download_warc_content(self, dataframe: dd.DataFrame) -> str:
        """Download the content of the warc files."""
        logger.info("Downloading common crawl files...")
        return dataframe.assign(content=dataframe.apply(
            lambda row: extract_bytes_from_warc_file_http(row["warc_filename"],
                                                            row["warc_record_offset"],
                                                            row["warc_record_length"]),
            axis=1,
            ),
        )

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

        ddf = ddf.dropna(subset=["content"])
        result_ddf = ddf[['url', 'content']]

        result_ddf.columns = [
            "webpage_url",
            "webpage_content",
        ]

        return result_ddf
