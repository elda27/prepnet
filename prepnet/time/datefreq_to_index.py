import pandas as pd
import numpy as np

from prepnet.core.config import get_config
from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.time.typing import AnyTimeDelta, AnyDateTime

class DateFreqToIndex(ColumnConverterBase):
    def __init__(
            self, freq_rule:AnyTimeDelta, 
            period_rule:AnyTimeDelta, 
            start:AnyDateTime=lambda x: x.min()
    ):
        super().__init__()
        self.freq = pd.to_timedelta(freq_rule)
        self.period = pd.to_timedelta(period_rule)
        self.start = start

    def encode(self, xs:pd.Series)->pd.Series:
        if get_config('keep_original'):
            self.original_xs = xs

        if callable(self.start):
            start = self.start(xs)
        else:
            start = self.start
        xs = np.clip(xs, start, None)
        self.date_range = pd.date_range(start, xs.max(), periods=self.period)
        
        index = np.digitize(xs, date_range)
        return pd.Series(
            index % (self.period / self.freq),
            index=xs.index, name=xs.name
        )

    def decode(self, xs:pd.Series)->pd.Series:
        if get_config('keep_original'):
            return self.original_xs
        else:
            return pd.Series(
                self.date_range.take(xs),
                index=xs.index, name=xs.name,
            )
