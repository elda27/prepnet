from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.column_converter_base import ColumnConverterBase

class Standardize(ColumnConverterBase):
    """Standardize to N(0, 1)
    """
    def encode(self, xs:pd.Series)->pd.Series:
        self.mean = xs.mean()
        self.std = xs.std()
        return (xs - self.mean) / self.std

    def decode(self, xs:pd.Series)->pd.Series:
        return xs * self.std + self.mean