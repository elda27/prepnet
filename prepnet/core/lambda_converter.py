import pandas as pd

from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.frame_converter_base import FrameConverterBase

class LambdaFrameConverter(FrameConverterBase):
    def __init__(self, encode, decode):
        """Conversion using lambda function
        """
        self.encode_method = encode
        self.decode_method = decode

    def encode(self, xs:pd.DataFrame):
        return self.encode_method(xs)

    def decode(self, xs:pd.DataFrame):
        return self.decode_method(xs)

class LambdaColumnConverter(ColumnConverterBase):
    def __init__(self, encode, decode):
        """Conversion using lambda function
        """
        self.encode_method = encode
        self.decode_method = decode

    def encode(self, xs:pd.Series):
        return self.encode_method(xs)

    def decode(self, xs:pd.Series):
        return self.decode_method(xs)
