"""Dataset creation pipeline config"""
from dataclasses import dataclass


@dataclass
class PipelineConfigs:
    """
    General Pipeline Configs
    Params:
        BASE_PATH (str): the base path used to store the artifacts
        HOST (str): the kfp host url
    """

    BASE_PATH = "/home/robbe/workspace/fondant/examples/pipelines/controlnet-interior-design/artifacts"
    HOST = "https://52074149b1563463-dot-europe-west1.pipelines.googleusercontent.com/"
