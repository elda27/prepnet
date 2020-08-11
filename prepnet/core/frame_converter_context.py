from typing import List, Dict
from queue import Queue
import asyncio

import pandas as pd

from prepnet.executor.state_value import StateValue
from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.frame_converter_base import FrameConverterBase

class FrameConverterContext(ColumnConverterBase):
    converters: Dict[FrameConverterBase, "FrameConverterContext"] = {}
    def __init__(self, frame_converter: FrameConverterBase):
        super().__init__()
        self.origin: FrameConverterBase = frame_converter
        if self.origin not in self.converters:
            self.converters[frame_converter] = self
            self.queued: Queue = Queue()
        else:
            self.queued = None

    async def encode_async(self, xs: pd.Series):
        if self.queued is None:
            await self.converters[self.origin].queue(xs)
        else:
            await self.queue(xs)
        yield StateValue.Queued

        if self.queued is not None:
            df = await self.concat_queue()
            async for i in self.origin.encode_async(df):
                yield i
        else:
            yield StateValue.Finished

    async def decode_async(self, xs: pd.Series):
        if self.queued is None:
            await self.converters[self.origin].queue(xs)
        else:
            await self.queue(xs)

        yield StateValue.Queued

        if self.queued is not None:
            df = await self.concat_queue()
            async for i in self.origin.decode_async(df):
                yield i
        else:
            yield StateValue.Queued


    def encode(self, xs:pd.Series):
        raise NotImplementedError()
    
    def decode(self, xs:pd.Series):
        raise NotImplementedError()

    async def queue(self, xs: pd.Series):
        if self.queued is None:
            return
        async with asyncio.Lock():
            self.queued.put(xs)

    async def concat_queue(self):
        async with asyncio.Lock():
            result = []
            while not self.queued.empty():
                result.append(self.queued.get())
            return pd.concat(result, axis=1)