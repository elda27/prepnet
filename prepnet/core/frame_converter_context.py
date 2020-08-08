from prepnet.executor.state_value import StateValue
from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.frame_converter_base import FrameConverterBase
import pandas as pd
from typing import List, Dict

class FrameConverterContext(ColumnConverterBase):
    converters: Dict[FrameConverterBase, "FrameConverterContext"] = {}
    def __init__(self, frame_converter: FrameConverterBase):
        super().__init__()
        self.origin: FrameConverterBase = frame_converter
        if self.origin not in self.converters:
            self.converters[frame_converter] = self
            self.queued: List[pd.Series] = []

    async def encode_async(self, xs: pd.Series):
        if self.origin in self.converters:
            self.converters[self.origin].queue(xs)
        yield StateValue.Queued
        df = pd.concat(self.queued, axis=1)
        async for i in self.origin.encode_async(df):
            yield i

    async def decode_async(self, xs: pd.Series):
        if self.origin in self.converters:
            self.converters[self.origin].queue(xs)
        yield StateValue.Queued
        df = pd.concat(self.queued, axis=1)
        async for i in self.origin.decode_async(df):
            yield i


    def encode(self, xs:pd.Series):
        raise NotImplementedError()
    
    def decode(self, xs:pd.Series):
        raise NotImplementedError()

    def queue(self, xs: pd.Series):
        if self.converters[self.origin] == self:
            self.queued = []
        self.queued.append(xs)