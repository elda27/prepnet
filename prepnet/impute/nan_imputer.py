from prepnet.core.config import get_config
from prepnet.core.frame_converter_base import FrameConverterBase
import pandas as pd

class NanImputer(FrameConverterBase):
    def __init__(self, value:float=0.0):
        super().__init__()
        self.value = value
        self.mask = None

    def encode(self, df:pd.DataFrame):
        if get_config('keep_original'):
            self.mask = df.isna()
            self.original = value
        return df.fillna(self.value)

    def decode(self, df:pd.DataFrame):
        if self.mask is not None:
            df = df.where(self.mask, value)
        return df
