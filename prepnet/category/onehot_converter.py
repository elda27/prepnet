from prepnet.core.config import get_config
from prepnet.core.frame_converter_base import FrameConverterBase
import pandas as pd

class OnehotConverter(FrameConverterBase):
    def __init__(self):
        super().__init__()
        self.mask = None

    def encode(self, df:pd.DataFrame):
        self.original_columns = df.columns
        self.original_dtypes = df.dtypes
        result = pd.get_dummies(df)
        return result

    def decode(self, df:pd.DataFrame):
        result = {}
        for col in self.original_columns:
            filtered_columns = list(filter(lambda x: x.startswith(col), df.columns))
            original_values = np.array([
                original_col[len(col)+1:]
                for original_col in filtered_columns
            ]).astype(self.original_dtypes[col])
            result[col] = df[filtered_columns].apply(
                lambda xs: sum(i * x for i, x in enumerate(xs))
            )

        return pd.concat([
            df, pd.DataFrame(result, index=df.index)
        ], axis=1)
