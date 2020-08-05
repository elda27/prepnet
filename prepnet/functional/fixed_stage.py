from typing import List
import pandas as pd

from prepnet.functional.function_configuration import FunctionConfiguration
from prepnet.executor.executor import Executor

class FixedStage:
    def __init__(
        self, stage_name:str, 
        configs:List[FunctionConfiguration], 
        enable:bool=True
    ):
        self.stage_name = stage_name
        self.stage_configurations:List[FunctionConfiguration] = []
        self.enable = enable

    def create_converters(self):
        return [
            config.create() 
            for config in self.stage_configurations
        ]

    def disable(self):
        stage = self.clone()
        stage.disable = False
        return stage

    def clone(self):
        stage = Stage(self.stage_name, enable=self.enable)
        stage.stage_configurations = [
            config.clone()
            for config in self.stage_configurations
        ]
        return stage