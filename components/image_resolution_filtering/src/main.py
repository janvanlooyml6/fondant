"""This component filters images of the dataset based on image size (minimum height and width)."""
import logging

import pandas as pd

from fondant.component import PandasTransformComponent
from fondant.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


class ImageFilterComponent(PandasTransformComponent):
    """Component that filters images based on height and width."""

    def setup(self, *, min_width: int, min_height: int) -> None:
        """
        Args:
            min_width: min width to filter on
            min_height: min height to filter on.
        """
        self.min_width = min_width
        self.min_height = min_height

    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        filtered_df = dataframe[
            (dataframe["images"]["width"] > self.min_width)
            & (dataframe["images"]["height"] > self.min_height)
        ]
        return filtered_df


if __name__ == "__main__":
    component = ImageFilterComponent.from_args()
    component.run()
