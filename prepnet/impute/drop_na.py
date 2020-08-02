from prepnet.core.config import get_config
from prepnet.core.frame_converter_base import FrameConverterBase
import pandas as pd

class DropNA(FrameConverterBase):
    def __init__(self):
        super().__init__()
        self.mask = None

    def encode(self, df:pd.DataFrame):
        if get_config('keep_original'):
            self.mask = df.isna()
            self.original = df[mask]
        return df.dropna()

    def decode(self, df:pd.DataFrame):
        if self.mask is not None:
            df = df.insert(self.mask.index, df.columns, self.original)
        return df
        