from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.column_converter_base import ColumnConverterBase

import pandas as pd

class Standardize(ColumnConverterBase):
    """Standardize to N(0, 1)
    """
    def __init__(self):
        super().__init__()
        self.mean = None
        self.std = None

    def encode(self, xs:pd.Series)->pd.Series:
        if self.mean is None:
            self.mean = xs.mean()
            self.std = xs.std()
        return (xs - self.mean) / self.std

    def decode(self, xs:pd.Series)->pd.Series:
        return xs * self.std + self.mean