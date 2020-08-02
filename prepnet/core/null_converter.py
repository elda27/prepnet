from prepnet.core.column_converter_base import ColumnConverterBase
class NullConverter(ColumnConverterBase):
    """None conversion
    """
    def __init__(self):
        self.origin = None

    def encode(self, xs:pd.Series):
        return xs

    def decode(self, xs:pd.Series):
        return xs
