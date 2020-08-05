from typing import List
from prepnet.functional.function_configuration import FunctionConfiguration

class ConfigurationContext:
    def __init__(self, columns: List[str]):
        self.columns: List[str] = columns
        self.converters = []

    def to_config(self)-> List[FunctionConfiguration]:
        configs = []
        for klass, args, kwargs in self.converters:
            configs.append(
                FunctionConfiguration(
                    self.columns, klass,
                    args, kwargs
                )
            )
        return config
    # TODO: Implementation of the function style preprocesses.
    # maybe automatic append by function annotation