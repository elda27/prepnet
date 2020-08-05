from typing import List

from prepnet.core.frame_converter_base import FrameConverterBase
from prepnet.core.frame_converter_context import FrameConverterContext
from prepnet.core.column_converter_base import ColumnConverterBase

class FunctionConfiguration:
    """Data class of the function configuration
    """
    def __init__(self, columns, klass, args, kwargs):
        self.args = args
        self.kwargs = kwargs
        self.converter_klass = klass
        self.columns = columns

    def create(self):
        if self.converter_klass is ColumnConverterBase:
            return {
                col: self.converter_klass(
                    *self.args, **self.kwargs
                ) for col in self.columns
            }
        elif self.converter_klass is FrameConverterBase:
            if self.columns is None:
                return self.converter_klass(
                    *self.args, **self.kwargs
                )
            else:
                return {
                    col: self.converter_klass(
                        *self.args, **self.kwargs
                    ) for col in self.columns
                }
    def clone(self):
        return FunctionConfiguration(
            self.columns, self.converter_klass,
            self.args, self.kwargs
        )