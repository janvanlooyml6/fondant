"""This component crops images by removing the borders."""
import io
import logging
import typing as t

import numpy as np
import pandas as pd
from image_crop import remove_borders
from PIL import Image

from fondant.component import PandasTransformComponent
from fondant.logger import configure_logging

configure_logging()
logger = logging.getLogger(__name__)


def extract_dimensions(image_df: pd.DataFrame) -> t.Tuple[np.int16, np.int16]:
    """Extract the width and height of an image.

    Args:
        image_df (dd.DataFrame): input dataframe with images_data column

    Returns:
        np.int16: width of the image
        np.int16: height of the image
    """
    image = Image.open(io.BytesIO(image_df["images_data"]))

    return np.int16(image.size[0]), np.int16(image.size[1])


class ImageCroppingComponent(PandasTransformComponent):
    """Component that crops images."""

    def setup(
        self,
        cropping_threshold: int = -30,
        padding: int = 10
    ) -> None:
        """
        Args:
            cropping_threshold: threshold parameter used for detecting borders
            padding: padding for the image cropping.
        """
        self.cropping_threshold = cropping_threshold
        self.padding = padding

    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        dataframe["images"]["data"] = dataframe["images"]["data"].apply(
            lambda x: remove_borders(x, self.cropping_threshold, self.padding),
            meta=(("images", "data"), "bytes"),
        )

        dataframe["images"][["width", "height"]] = dataframe["images"]["data"].apply(
            extract_dimensions,
        )

        return dataframe


if __name__ == "__main__":
    component = ImageCroppingComponent.from_args()
    component.run()
