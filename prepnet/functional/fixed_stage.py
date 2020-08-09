from typing import List, Dict
from collections import defaultdict

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
        self.stage_configurations:List[FunctionConfiguration] = configs
        self.enable = enable

    def create_converters(self):
        all_converters = defaultdict(list)
        all_converters_array = []
        for config in self.stage_configurations:
            converters = config.create()
            if isinstance(converters, dict):
                for col, converter in converters.items():
                    all_converters[col].append(converter)
            else:
                all_converters_array.append(converters)
        assert not (len(all_converters) > 0 and len(all_converters_array) > 0), \
            'All converter should only be FrameConverter or ColumnConverter.'

        return all_converters

    def disable(self):
        stage = self.clone()
        stage.enable = False
        return stage

    def clone(self):
        stage = FixedStage(self.stage_name, enable=self.enable)
        stage.stage_configurations = [
            config.clone()
            for config in self.stage_configurations
        ]
        return stage