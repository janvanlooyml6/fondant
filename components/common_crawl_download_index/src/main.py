"""A component that downloads common crawl files."""
import logging
import typing as t

import dask
import dask.dataframe as dd
from configs import VALID_COMMONCRAWL_INDEX_FILTERS, VALID_OPERATORS
from fondant.component import DaskLoadComponent

logger = logging.getLogger(__name__)

dask.config.set(scheduler="processes")


class CommonCrawlDownloadComponent(DaskLoadComponent):
    """Component that download common crawl files."""

    def __init__(
        self,
        *_,
        common_crawl_indices: t.List[str],
        filters: t.List[dict],
    ):
        self.filters = parse_commoncrawl_index_filters(filters) if filters else None
        self.index_files = common_crawl_indices

    def load(self) -> dd.DataFrame:
        """
        Load the common crawl index with the provided filters.

        Returns:
            A dataframe containing the filtered urls and their location in the WARC archivei
        """
        logger.info("Loading filtered common crawl index...")

        output_columns = [
            "url_surtkey",
            "url",
            "warc_filename",
            "warc_record_offset",
            "warc_record_length",
        ]

        dataframe = dd.read_parquet(
            self.index_files,
            filters=self.filters,
            columns=output_columns,
        )

        dataframe = dataframe.set_index("url_surtkey", sorted=True, drop=True)
        dataframe.columns = [
            "webpage_url",
            "webpage_filename",
            "webpage_offset",
            "webpage_length",
        ]

        dataframe = dataframe.repartition(npartitions=1000 * len(self.index_files))

        return dataframe


def parse_commoncrawl_index_filters(filters: t.List) -> t.List:
    """Parse and validate the provided filters."""
    logger.info(f"Validating filters: {filters}")

    if not isinstance(filters, list):
        msg = "filters must be a list"
        raise TypeError(msg)

    filters = [(d["field"], d["operator"], d["value"]) for d in filters]

    for filter_tuple in filters:
        if not isinstance(filter_tuple, tuple):
            msg = "filters must be a list of tuples"
            raise TypeError(msg)

        field, operator, value = filter_tuple
        if field not in VALID_COMMONCRAWL_INDEX_FILTERS:
            msg = f"Invalid field: {field} in filter expression: {filter_tuple}"
            raise ValueError(msg)

        if operator not in VALID_OPERATORS:
            msg = f"Invalid operator: {operator} in filter expression: {filter_tuple}"
            raise ValueError(msg)

    return filters
