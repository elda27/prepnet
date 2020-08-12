from prepnet.executor.executor_base import ExecutorBase
from prepnet.core.frame_converter_base import FrameConverterBase
from typing import List, Dict

import pandas as pd

class FrameExecutor(ExecutorBase):
    def __init__(self, converters:List[FrameConverterBase], columns: List[str]=None):
        self.columns = list(columns)
        self.result_columns = None
        self.converters = converters

    def encode(self, df: pd.DataFrame):
        if self.columns is None:
            self.columns = df.columns
        input_df = df.drop(columns=self.columns)
        df = df[self.columns]
        for converter in self.converters:
            df = converter.encode(df)
        if self.result_columns is None:
            self.result_columns = df.columns
        return pd.concat([input_df, df], axis=1)

    def decode(self, df: pd.DataFrame):
        assert self.result_columns is not None
        input_df = df.drop(columns=self.result_columns)
        df = df[self.result_columns]
        for converter in self.converters:
            df = converter.decode(df)
        return pd.concat([input_df, df], axis=1)
