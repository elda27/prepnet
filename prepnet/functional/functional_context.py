from contextlib import contextmanager
from typing import Any, List
from copy import deepcopy

from prepnet.executor.executor import Executor
from prepnet.functional.fixed_stage import FixedStage
from prepnet.functional.configuration_context import ConfigurationContext

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
        self.current_stage_contexts: List[ConfigurationContext] = []
        self.stage_index:int = 1
        self.stage_converters = None

    @contextmanager
    def enter(self, stage:Any=None)->FunctionalContext:
        if stage is None:
            stage = self.stage_index
            self.stage_index += 1
        old_stage_name = self.stage_name
        old_stage = self.current_stage_contexts 
        
        try:
            yield self
        finally:
            self.stages.append(
                FixedStage(
                    self.stage_name,
                    [context.to_config() for context in self.current_stage_contexts]
                )
            )
            self.stage_name = old_stage_name
            self.current_stage_contexts = old_stage

    def __getitem__(self, *keys):
        context = ConfigurationContext(keys)
        self.current_stage_contexts.append(context)
        return context

    def __getattribute__(self, name):
        context = ConfigurationContext(None)
        self.current_stage_contexts.append(context)
        return getattr(context, name)
    
    def create_converters(self):
        self.stage_converters = [
            stage.create_converter()
            for stage in self.stages
            if stage.enable
        ]

    def encode(self, df: pd.DataFrame):
        if self.converters is None:
            self.stage_converters = self.create_converters()

        for converters in self.stage_converters:
            df = Executor(converters).encode(df)
        return df

    def decode(self, df: pd.DataFrame):
        assert self.stage_converters is not None
        for converters in reversed(self.stage_converters):
            df = Executor(converters).encode(df)
        return df
