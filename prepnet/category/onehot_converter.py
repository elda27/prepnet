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
        self.autocast = autocast
        self.result_columns = None

    def encode(self, df:pd.DataFrame):
        self.original_columns = df.columns
        self.original_dtypes = df.dtypes

        if self.autocast:
            df = df.apply({
                col: (lambda x: x) if dtype.kind == 'O' else (lambda x: x.astype(str))
                for col, dtype in df.dtypes.items()
            })
        else:
            assert all([dtype.kind == 'O' for dtype in df.dtypes.values()]), \
                'Onehot encoding only support str columns'

        result = pd.get_dummies(df)
        if self.result_columns is not None:
            reminder_columns = list(filter(
                lambda x: x not in result.columns,
                self.result_columns
            ))
            result[reminder_columns] = np.uint8(0)
        else:
            self.result_columns = result.columns
        return result

    def decode(self, df:pd.DataFrame):
        result = {}
        for col in self.original_columns:
            filtered_columns = list(filter(lambda x: x.startswith(col), df.columns))
            if not get_config('raise_satisfied') and len(filtered_columns) == 0:
                break

            reconstructed = df[filtered_columns].idxmax(axis=1)
            series =  reconstructed.apply(lambda x: x[len(col) + 1:])
            if series.dtype != self.original_dtypes[col]:
                series = series.astype(self.original_dtypes[col])
            result[col] = series
        return pd.DataFrame(result, index=df.index)
