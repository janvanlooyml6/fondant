from configparser import ConfigParser
from io import BytesIO

import httpx
import trafilatura
from warcio.archiveiterator import ArchiveIterator


def extract_bytes_from_warc_file_http(warc_filename: str, record_offset: int, record_length: int,
                                      prefix: str = "https://data.commoncrawl.org/"):
    """Extracts a specified range of bytes from a WARC file over HTTP."""
    url = f"{prefix}{warc_filename}"
    headers = {"Range": f"bytes={record_offset}-{record_offset + record_length - 1}"}

    with httpx.Client() as client:
        response = client.get(url, headers=headers)

    with BytesIO(response.content) as warc_stream:
        for record in ArchiveIterator(warc_stream):
            if "response" in record.content_type:
                return record.content_stream().read()
    return None


def extract_html(content: str, trafilatura_config: ConfigParser) -> str:
    """Extracts structured data from HTML content using Trafilatura."""
    return trafilatura.extract(
        content,
        no_fallback=True,
        include_tables=False,
        deduplicate=True,
        config=trafilatura_config,
    )
