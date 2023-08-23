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
    set_unique_index,
    validate_commoncrawl_index_filters,
)

logger = logging.getLogger(__name__)

CC_INDEX_PATH = "s3://commoncrawl/cc-index/table/cc-main/warc/"
CC_BASE_URL = "s3://"

class CommonCrawlDownloadComponent(DaskLoadComponent):
    """Component that download common crawl files."""

    def __init__(self,
                 *_,
                 crawl_name: str,
                 filters: List,
                 extract_plain_text: bool,
                 n_records_to_download: Optional[int] = None):
        # init global trafilatura config for multi-processing
        self.trafilatura_config = use_config()
        self.trafilatura_config.set("DEFAULT", "EXTRACTION_TIMEOUT", "0")

        self.crawl_name = crawl_name
        self.filters = validate_commoncrawl_index_filters(filters)
        self.extract_plain_text = extract_plain_text
        self.n_records_to_download = n_records_to_download


    def filter_common_crawl_index(self) -> dd.DataFrame:
        """Use common crawl index and provided filters to retrieve relevant pages
        from common crawl.
        """
        logger.info("Filtering common crawl index...")
        output_columns = [
            "url",
            "warc_filename",
            "warc_record_offset",
            "warc_record_length",
        ]

        cc_index = ds.dataset(CC_INDEX_PATH, format="parquet", partitioning="hive")

        crawl_subset = f"crawl={self.crawl_name}/subset=warc/"
        warc_files = [f for f in cc_index.files if crawl_subset in f]
        warc_files = [CC_BASE_URL + file for file in warc_files] # TODO: change to http
        warc_ddf = dd.read_parquet(
            warc_files,
            engine='pyarrow',
            filters=self.filters,
            columns=output_columns,
        )

        # optional: limit number of records to download
        if self.n_records_to_download is not None:
            partitions_length = 0
            npartitions = 1
            for npartitions, partition in enumerate(warc_ddf.partitions, 1):
                if partitions_length >= self.n_records_to_download:
                    logger.info(f"""Required number of partitions to load\n
                    {self.n_records_to_download} is {npartitions}""")
                    break
                partitions_length += len(partition)
                warc_ddf = warc_ddf.head(self.n_records_to_download, npartitions=npartitions)
                warc_ddf = dd.from_pandas(warc_ddf, npartitions=npartitions)

        # set index
        warc_ddf = warc_ddf.map_partitions(set_unique_index, meta=warc_ddf.head())

        return warc_ddf


    def download_warc_content(self, dataframe: dd.DataFrame) -> str:
        """Download the content of the warc files."""
        logger.info("Downloading common crawl files...")
        return dataframe.assign(content=dataframe.apply(
            lambda row: extract_bytes_from_warc_file_http(row["warc_filename"],
                                                            row["warc_record_offset"],
                                                            row["warc_record_length"]),
            axis=1,
            meta=("content", "str"),
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

        ddf = ddf.dropna(subset=["content"])
        if self.extract_plain_text:
            ddf["content"] = ddf["content"].apply(
                lambda x: extract_html(x, self.trafilatura_config),
                meta=("url", "str"),
            )

        result_ddf = ddf[['url', 'content']]

        result_ddf.columns = [
            "webpage_url",
            "webpage_content",
        ]

        return result_ddf
