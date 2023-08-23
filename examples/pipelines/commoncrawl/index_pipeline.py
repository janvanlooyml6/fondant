import logging
import sys

sys.path.append("../../..")
from fondant.pipeline import ComponentOp, Pipeline

logger = logging.getLogger(__name__)

# initialize pipeline
commoncrawl_pipeline = Pipeline(
    pipeline_name="commoncrawl_pipeline",
    base_path="/Users/sharongrundmann/Projects/fondant/fondant_artifacts",
    pipeline_description="A pipeline for downloading Common crawl files.",
)

# define ops
common_crawl_download_op = ComponentOp(
    # component_dir="components/common_crawl_download",
    component_dir="/Users/sharongrundmann/Projects/fondant/components/common_crawl_download",
    arguments={
        "crawl_name": "CC-MAIN-2023-14",
        "filters": [
            # {"field": "crawl", "operator": "==", "value": "CC-MAIN-2023-14"},
            # {"field": "subset", "operator": "==", "value": "warc"},
            {"field": "content_mime_type", "operator": "==", "value": "text/html"},
            {"field": "content_languages", "operator": "==", "value": "eng"},
        ],
        "extract_plain_text": True,
        "n_records_to_download": 10,
    },
)

# add ops to pipeline
commoncrawl_pipeline.add_op(common_crawl_download_op)
