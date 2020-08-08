from prepnet.executor.state_value import StateValue
from prepnet.executor.state_manager import StateManager
from prepnet.executor.exec_mode import ExecMode
from prepnet.core.column_converter_base import ColumnConverterBase

import asyncio
from enum import Enum
import pandas as pd

class Executor:
    exec_mode_dict = {
        ExecMode.DecodeAsync: 'decode_async',
        ExecMode.EncodeAsync: 'encode_async',
    }
    def __init__(self, converters, loop=None):
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.status = StateManager(converters)
        self.converters = converters

    async def exec_async(self, df: pd.DataFrame, mode: ExecMode):
        generator_dict = {}
        for col, converter in self.converters.items():
            self.status.run(converter)
            generator_dict[col] = (converter, getattr(converter, self.exec_mode_dict[mode])(df[col]))

        while not self.status.is_all_finished():
            for col, (converter, generator) in generator_dict.items():
                if not self.status.is_states(converter, (StateValue.Running, StateValue.Prepared)):
                    continue
                self.status.run(converter)
                result = await generator.__anext__()
                if isinstance(result, (pd.DataFrame, pd.Series)):
                    self.status.finish(converter)
                    df = self._assign(df, col, result)
                elif isinstance(result, self.StateValue):
                    self.status.queue(converter)
                else:
                    raise ValueError(f'Yield unsupported value type: {type(result)}')

            if self.status.is_all_queued():
                self.status.set_prepare()
        return df

    def exec(self, df: pd.DataFrame, mode: ExecMode):
        return self.loop.run_until_complete(self.exec_async(df, mode))

    def encode(self, df: pd.DataFrame):
        return self.exec(df, ExecMode.EncodeAsync)

    def decode(self, df: pd.DataFrame):
        return self.exec(df, ExecMode.DecodeAsync)

    def _assign(self, df, col, result):
        if isinstance(result, pd.DataFrame):
            return df.drop(col).assign(result)
        else:
            return df.assign(**{col:result})