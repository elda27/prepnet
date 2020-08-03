from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.executor.state_value import StateValue
from typing import List

class SequenceConverter(ColumnConverterBase):
    def __init__(self, converters: List[ColumnConverterBase]):
        super().__init__()
        self.converters = converters

    async def encode_async(self, xs):
        for conv in self.converters:
            result = await conv.encode_async(xs)
            if isinstance(result, StateValue):
                yield result
            else:
                xs = result
        yield xs

    async def decode_async(self, ys):
        for conv in self.converters:
            result = await conv.decode_async(xs)
            if isinstance(result, StateValue):
                yield result
            else:
                xs = result
        yield xs

    def encode(self, xs):
        for conv in self.converters:
            xs = conv.encode(xs)
        return xs

    def decode(self, ys):
        for conv in reversed(self.converters):
            xs = conv.decode(xs)
        return xs

