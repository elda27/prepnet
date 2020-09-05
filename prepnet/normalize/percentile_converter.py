from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.config import get_config

import numpy as np
import pandas as pd

class PercentileConverter(ColumnConverterBase):
    def __init__(self):
        """Normalize by qunatile value
        
        Examples:
        >>> input_series = pd.Series(np.arange(10)).sample(frac=1)
        >>> input_series.to_list()
        [0, 2, 7, 8, 6, 4, 1, 9, 5, 3]
        >>> PercentileConverter().encode()
        [0.0,
         0.2222222222222222,
         0.7777777777777778,
         0.8888888888888888,
         0.6666666666666666,
         0.4444444444444444,
         0.1111111111111111,
         1.0,
         0.5555555555555556,
         0.3333333333333333
        ]
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