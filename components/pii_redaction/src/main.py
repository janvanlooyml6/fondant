"""A component that detects and redacts Personal Identifiable Information (PII) in code."""

import json
import logging

import pandas as pd
from pii_detection import scan_pii
from pii_redaction import redact_pii

from fondant.component import PandasTransformComponent
from fondant.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


class RemovePIIComponent(PandasTransformComponent):
    """Component that detects and redacts PII from code."""

    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["code"][["secrets", "has_secrets", "number_secrets"]] = \
            dataframe["code"]["content"].apply(scan_pii)

        with open("replacements.json", "r") as f:
            replacements = json.load(f)

        dataframe["code"]["content"] = dataframe["code"].apply(
            lambda example: redact_pii(
                text=example.content,
                secrets=example.secrets,
                has_secrets=example.has_secrets,
                replacements=replacements,
            )
        )
        return dataframe["code"].drop(
            ["secrets", "has_secrets", "number_secrets"], axis=1
        )


if __name__ == "__main__":
    component = RemovePIIComponent.from_args()
    component.run()
