"""This component filters image-text pairs based on a text spotting model.

Basically, all images whose recognized text appears in the caption get filtered out.
"""
import logging

import cv2
import pandas as pd
import torch

from huggingface_hub import hf_hub_download
import onnxruntime as ort

from easyocr.craft_utils import getDetBoxes, adjustResultCoordinates
from easyocr.imgproc import resize_aspect_ratio, normalizeMeanVariance
from easyocr.utils import reformat_input

from fondant.component import PandasTransformComponent

logger = logging.getLogger(__name__)


def get_boxes(session, url):
    img, _ = reformat_input(url)

    # Resize and normalize input image
    img_resized, target_ratio, size_heatmap = resize_aspect_ratio(
        img, 512, interpolation=cv2.INTER_LINEAR, mag_ratio=1.0
    )
    ratio_h = ratio_w = 1 / target_ratio
    x = normalizeMeanVariance(img_resized)
    x = torch.from_numpy(x).permute(2, 0, 1).unsqueeze(0)

    input_name = session.get_inputs()[0].name

    # Prepare input tensor for inference
    inp = {input_name: x.numpy()}

    # Run inference and get output
    y, _ = session.run(None, inp)

    # Extract score and link maps
    score_text = y[0, :, :, 0]
    score_link = y[0, :, :, 1]

    # Post-processing to obtain bounding boxes and polygons
    boxes, _, _ = getDetBoxes(score_text, score_link, 0.5, 0.4, 0.4)
    boxes = adjustResultCoordinates(boxes, ratio_w, ratio_h)

    return boxes


class FilterTextSpottingComponent(PandasTransformComponent):
    """Component that filters image-text pairs based on a text spotting model."""

    def setup(self) -> None:
        onnx_file = hf_hub_download(
            repo_id="ml6team/craft-onnx", filename="craft.onnx", repo_type="model"
        )

        # TODO support CUDA using onnxruntime-gpu
        # providers = [('CUDAExecutionProvider', {"cudnn_conv_algo_search": "DEFAULT"}), 'CPUExecutionProvider'] if ort.get_device() == 'GPU' else ['CPUExecutionProvider']
        providers = ["CPUExecutionProvider"]
        self.session = ort.InferenceSession(onnx_file, providers=providers)

    def transform(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """
        Args:
            dataframe: Pandas dataframe

        Returns:
            Pandas dataframe
        """
        dataframe["image_boxes"] = dataframe["image"]["url"].apply(
            lambda example: get_boxes(self.session, example)
        )

        return dataframe


if __name__ == "__main__":
    component = FilterTextSpottingComponent.from_args()
    component.run()
