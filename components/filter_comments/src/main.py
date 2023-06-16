"""
This component estimates the code to comments ratio and filters instances between two chosen
minimum and maximum values.
"""
import logging

import pandas as pd
from utils.text_extraction import get_comments_to_code_ratio

from fondant.component import TransformComponent
from fondant.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


class FilterCommentsComponent(TransformComponent):
    """Component that filters instances based on code to comments ratio."""

    def setup(self, *, min_comments_ratio: float, max_comments_ratio: float) -> None:
        """
        Args:
            min_comments_ratio: The minimum code to comment ratio
            max_comments_ratio: The maximum code to comment ratio.
        """
        self.min_comments_ratio = min_comments_ratio
        self.max_comments_ratio = max_comments_ratio

    def transform(
        self,
        dataframe: pd.DataFrame,
    ) -> pd.DataFrame:
        mask = dataframe["code"]["content"].map(
            get_comments_to_code_ratio,
        ).between(
            self.min_comments_ratio, self.max_comments_ratio
        )
        filtered_df = dataframe[mask]

        return filtered_df


if __name__ == "__main__":
    component = FilterCommentsComponent.from_args()
    component.run()
