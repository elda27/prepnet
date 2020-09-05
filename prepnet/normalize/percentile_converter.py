from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.config import get_config

import numpy as np
import pandas as pd

class PercentileConverter(ColumnConverterBase):
    def __init__(self):
        """Normalize by qunatile value
        """

        super().__init__()
        self.original_xs = None

    def encode(self, xs:pd.Series)->pd.Series:
        if self.original_xs is None:
            self.original_xs = xs

        mask = xs.to_numpy()[np.newaxis] > self.original_xs.to_numpy()[:, np.newaxis]
        quantile = pd.Series(
            np.count_nonzero(mask, axis=0) / (len(self.original_xs) - 1),
            index=xs.index
        )
        quantile.name = xs.name
        return quantile 

    def decode(self, xs:pd.Series)->pd.Series:
        return self.original_xs.sort_values().take(
            (xs * (len(self.original_xs) - 1)).astype(np.int32)
        )