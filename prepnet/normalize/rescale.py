from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.config import get_config

import pandas as pd

class Rescale(ColumnConverterBase):
    def __init__(self, lower:float=None, upper:float=None, dst_min:float=0.0, dst_max:float=1.0):
        """Recale 0.0 to 1.0 by min and max value.

        Args:
            lower (float, optional): Lower bound of rescale. If None, 
                the value will be input min element. Defailts to None.
            upper (float, optional): Upper bound of rescale. If None, 
                the value will be input max element. Defailts to None.
            dst_min (float, optional): Minimum value after rescale. Defaults to 0.0.
            dst_max (float, optional): Maximum value after rescale. Defaults to 1.0.
        """

        super().__init__()
        self.lower = lower
        self.upper = upper
        self.dst_min = dst_min
        self.dst_max = dst_max
        self.original_xs = None

    def encode(self, xs:pd.Series)->pd.Series:
        if self.upper is None:
            self.upper = xs.max()
        if self.lower is None:
            self.lower = xs.min()
        
        if get_config('keep_original'):
            self.original_xs = xs[xs.between(self.lower, self.upper)]

        xs = xs.clip(self.lower, self.upper)
        xs = (xs - self.lower) / (self.upper - self.lower)

        return xs * (self.dst_max - self.dst_min) - self.dst_min

    def decode(self, xs:pd.Series)->pd.Series:
        xs = (xs + self.dst_min) / (self.dst_max - self.dst_min)
        xs = xs * (self.upper - self.lower) + self.lower
        if self.original_xs is not None:
            xs[self.original_xs.index] = self.original_xs
        return xs