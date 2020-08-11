from typing import Dict, List

from prepnet.functional.function_configuration import FunctionConfiguration
from prepnet.category.onehot_converter import OnehotConverter
from prepnet.category.ordinal_converter import OrdinalConverter

class ConfigurationContext:
    def __init__(self, columns: List[str]):
        self.columns: List[str] = columns
        self.converters = []

    def to_config(self)->List[FunctionConfiguration]:
        configs = []
        for klass, args, kwargs in self.converters:
            configs.append(
                FunctionConfiguration(
                    self.columns, klass,
                    args, kwargs
                )
            )
        return configs

    def add_config(self, klass, *args, **kwargs):
        self.converters.append((klass, args, kwargs))

    def onehot(self):
        self.add_config(OnehotConverter)
        return self

    def ordinal(self):
        self.add_config(OrdinalConverter)
        return self
