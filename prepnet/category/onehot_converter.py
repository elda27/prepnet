from prepnet.core.config import get_config
from prepnet.core.frame_converter_base import FrameConverterBase
import pandas as pd
import numpy as np


class OnehotConverter(FrameConverterBase):
    def __init__(self, autocast:bool=True):
        """Onehot encoding for categorical columns.

        Args:
            autocast (bool): Each columns are automatically cast to string type.
        """
        super().__init__()
        self.result_columns = None

    def encode(self, df:pd.DataFrame):
        self.original_columns = df.columns
        self.original_dtypes = df.dtypes

        df = df.apply({
            col: lambda x: x if dtype.kind == 'O' else col:lambda x: x.astype(str)
            for col, dtype in df.dtypes.items()
        })

        result = pd.get_dummies(df)
        
        assert self.result_columns is None or (self.result_columns == df.columns).all()

        self.result_columns = result.columns
        return result

    def decode(self, df:pd.DataFrame):
        result = {}
        for col in self.original_columns:
            filtered_columns = list(filter(lambda x: x.startswith(col), df.columns))
            reconstructed = df[filtered_columns].idxmax(axis=1)
            result[col] =  reconstructed.apply(lambda x: x[len(col) + 1:])

        return pd.DataFrame(result, index=df.index)
