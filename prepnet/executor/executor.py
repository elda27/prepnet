import asyncio
from typing import Dict, Union, List
from enum import Enum

from prepnet.executor.state_value import StateValue
from prepnet.executor.state_manager import StateManager
from prepnet.executor.exec_mode import ExecMode
from prepnet.core.column_converter_base import ColumnConverterBase
from prepnet.core.sequence_converter import SequenceConverter

import pandas as pd

ConvertersAnnotation = Dict[str, ColumnConverterBase]

class Executor:
    exec_mode_dict = {
        ExecMode.DecodeAsync: 'decode_async',
        ExecMode.EncodeAsync: 'encode_async',
    }
    def __init__(self, converters, loop=None):
        self.loop = asyncio.get_event_loop() if loop is None else loop
        self.converters = {}
        for key, converter in converters.items():
            if isinstance(converter, list):
                self.converters[key] = SequenceConverter(converter)
            else:
                self.converters[key] = converter 

        self.status: StateManager = StateManager(self.converters)
        self.column_converter = {}

    async def exec_async(self, df: pd.DataFrame, mode: ExecMode):
        generator_dict = {}
        for col, converter in self.converters.items():
            self.status.run(converter)
            if mode == ExecMode.DecodeAsync:
                input_df = df[self.column_converter.get(col, col)]
            else:
                input_df = df[col]
            generator = getattr(converter, self.exec_mode_dict[mode])(input_df)
            generator_dict[col] = (converter, generator)

        while not self.status.is_all_finished():
            for col, (converter, generator) in generator_dict.items():
                if not self.status.is_states(converter, (StateValue.Running, StateValue.Prepared)):
                    continue
                self.status.run(converter)
                try:
                    result = await generator.__anext__()
                except:
                    self.status.finish(converter)
                    continue

                if isinstance(result, (pd.DataFrame, pd.Series)):
                    if mode == ExecMode.DecodeAsync:
                        src_col = self.column_converter.get(col, col)
                    else:
                        if isinstance(result, pd.Series):
                            self.column_converter[col] = result.name
                        else:
                            self.column_converter[col] = result.columns
                        src_col = col
                    self.status.finish(converter)
                    df = self._assign(
                        df, src_col,
                        result
                    )
                elif isinstance(result, StateValue):
                    self.status.set_status(converter, result)
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
            return pd.concat([df.drop(columns=col), result], axis=1)
        else:
            return df.assign(**{col:result})