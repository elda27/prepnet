from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.frame_converter_base import FrameConverterBase
from typing import List
import pandas as pd

class SequenceConverter(ColumnConverterBase):
    def __init__(self, converters: List[ColumnConverterBase]):
        super().__init__()
        self.converters = converters

    def encode(self, xs):
        for conv in self.converters:
            if issubclass(type(conv), FrameConverterBase):
                xs = self.exec_frame_wise(conv.encode, xs)
            elif issubclass(type(conv), ColumnConverterBase):
                xs = self.exec_column_wise(conv.encode, xs)
            else:
                raise TypeError(f'Unknown converter type: {type(conv)}')
        return xs

    def decode(self, xs):
        for conv in reversed(self.converters):
            xs = conv.decode(xs)
        return xs

    def exec_column_wise(self, convert_method, xs):
        if isinstance(xs, pd.Series):
            return convert_method(xs)
        elif isinstance(xs, pd.DataFrame):
            return pd.DataFrame([
                convert_method(xs[col])
                for col in xs.columns
            ])
        else:
            raise ValueError(
                "DataFrameArray is not supported under the column wise conversion"
            )

    def exec_frame_wise(self, convert_method, xs):
        if isinstance(xs, pd.Series):
            result = convert_method(pd.DataFrame(xs))[
                xs.name if xs.name is not None else 0
            ]
            result.name = xs.name
            return result
        elif isinstance(xs, pd.DataFrame):
            return convert_method(xs)
        else:
            raise ValueError(
                "DataFrameArray is not supported under the column wise conversion"
            )

