from prepnet.executor.state_manager import StateManager
from prepnet.core.column_converter_base import ColumnConverterBase
import asyncio
from enum import Enum
import pandas as pd

class Executor:
    StateValue = StgateManager.StateValue

    def __init__(self, converters):
        self.status = StateManager(converters)
        self.converters = converters

    async def exec_async(self, df: pd.DataFrame, mode: Mode=Mode.Encode ):
        generator_dict = {}
        for col, converter in self.converters.items():
            self.status.run(converter)
            generator_dict[col] = (converter, await getattr(converter, mode)(df[col]))

        while not self.status.is_all_finished():
            for col, (converter, generator) in generator_dict.items():
                if not self.status.is_prepared(converter) or self.status.is_running(converter):
                    continue
                self.status.run(converter)
                result = await next(generator)
                if isinstance(result, (pd.DataFrame, pd.Series)):
                    self.status.finish(converter)
                    self._assign(df, col, result)
                elif isinstance(result, self.StateValue):
                    self.status.queue(converter)
                else:
                    raise ValueError(f'Yield unsupported value type: {type(result)}')

            if self.status.is_all_queued():
                self.status.set_prepare()

    def exec(self, df: pd.DataFrame, mode: Mode):
        loop = asyncio.get_event_loop() if loop is None else loop
        return loop.run_until_complete(self.exec_async(df))

    def encode(self, df: pd.DataFrame):
        return self.exec(df, Mode.Encode)

    def decode(self, df: pd.DataFrame):
        return self.exec(df, Mode.Decode)

    def _assign(self, df, col, result):
        if isinstance(result, pd.DataFrame):
            return df.drop(col).assign(result)
        else:
            return df.assign({col:result})