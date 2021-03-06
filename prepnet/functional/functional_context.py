from contextlib import contextmanager
from typing import Any, List
from copy import deepcopy

import pandas as pd

from prepnet.executor.executor import Executor
from prepnet.functional.fixed_stage import FixedStage
from prepnet.functional.configuration_context import ConfigurationContext
from prepnet.functional.frame_context import FrameContext
from prepnet.core.null_converter import NullConverter

class FunctionalContext:
    """Functional style preprocess

    Examples:
        >>> context = FunctionalContext()
        >>> # Registering preprocesses
        >>> with context.enter() as f:
        >>>     f['tom'].ordinal()
        >>>     f['becky'].quantile(0.99).standardize()
        >>> with context.enter('secondaly') as f:
        >>>     f.split([0.8, 0.2], shuffle=True) # Apply frame context without accessor
        >>> context.encode(other_df) # Registered preprocesses will be applied.
        >>> context.disable('secondaly').encode(alt_df) # Split method will not be applied.
        >>> # Decoding preprocess can same as encode...
    """
    def __init__(self):
        self.stages: List[FixedStage] = []
        
        self.stage_name: str = '0'
        self.stage_index:int = 1
        
        self.current_stage_contexts: List[ConfigurationContext] = []

        self.stage_executors = None
        self.result_columns = None

    @contextmanager
    def enter(self, stage_name:str=None)->"FunctionalContext":
        old_stage_name = self.stage_name
        old_stage = self.current_stage_contexts 
        
        if stage_name is None:
            self.stage_name = str(self.stage_index)
            self.stage_index += 1
        else:
            self.stage_name = stage_name
        self.current_stage_contexts  = []
        
        try:
            yield self
        finally:
            self.stages.append(
                FixedStage(
                    self.stage_name, 
                    [context for context in self.current_stage_contexts]
                )
            )
            self.stage_name = old_stage_name
            self.current_stage_contexts = old_stage

    def __getitem__(self, keys)->ConfigurationContext:
        if isinstance(keys, tuple):
            keys = list(keys)
        elif isinstance(keys, str):
            keys = [keys]
        context = ConfigurationContext(keys)
        self.current_stage_contexts.append(context)
        return context

    def __getattr__(self, name):
        context = ConfigurationContext(None)
        self.current_stage_contexts.append(context)
        return getattr(context, name)
    

    def create_converters(self):
        return [
            stage.create_converters()
            for stage in self.stages
            if stage.enable
        ]

    def disable(self, *keys)->"FunctionalContext":
        obj = self.clone()
        obj.stages = [
            stage.disable() if stage.stage_name in keys else stage
            for stage in obj.stages
        ]
        
        return obj

    def clone(self)->"FunctionalContext":
        context = FunctionalContext()
        context.stages = [
            stage.clone() for stage in self.stages
        ]
        
        context.stage_name = self.stage_name
        context.stage_index = self.stage_index
        
        return context

    def encode(self, df: pd.DataFrame):
        if self.stage_executors is None:
            self.stage_executors = []
            stage_converters = self.create_converters()
            for converters in stage_converters: # Create executor
                self.stage_executors.append(Executor(converters))

        for executor in self.stage_executors:
            df = executor.encode(df)
        self.result_columns = df.columns
        return df

    def decode(self, df: pd.DataFrame):
        assert self.stage_executors is not None
        result_columns = self.result_columns
        for executor in reversed(self.stage_executors):
            df = executor.decode(df)
        return df
