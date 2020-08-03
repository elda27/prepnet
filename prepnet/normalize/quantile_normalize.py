from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.config import get_config

import pandas as pd

class QuantileNormalize(ColumnConverterBase):
    """N-percentile outlier will be removed
    """
    def __init__(self, percentile=0.99):
        super().__init__()
        self.percentile = percentile
        self.mask = None

    def encode(self, xs:pd.Series)->pd.Series:
        upper = xs.quantile(self.percentile)
        lower = xs.quantile(1.0 - self.percentile)

        upper_mask = xs < upper
        lower_mask = xs > lower

        if get_config('keep_original'):
            self.mask = lower_mask
            self.original_values = xs[self.mask]

        xs = self.where(upper_mask, upper)
        xs = self.where(lower_mask, lower)

        return xs

    def decode(self, xs:pd.Series)->pd.Series:
        if self.mask is not None:
            xs = xs.mask(self.mask, self.original_values)
        return xs
