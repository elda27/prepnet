from prepnet.core.config import get_config
from prepnet.core.column_converter_base import ColumnConverterBase
import pandas as pd

class OrdinalConverter(ColumnConverterBase):
    def __init__(self, na_value=-1):
        super().__init__()
        self.na_value = na_value

    def encode(self, xs:pd.Series):
        codes, self.uniques = pd.factorize(xs, na_sentinel=self.na_value)
        return pd.Series(codes, index=xs.index)

    def decode(self, xs:pd.Series):
        ys = pd.Series(self.uniques.take(xs))
        ys.index = xs.index
        return  ys