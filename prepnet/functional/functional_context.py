from contextlib import contextmanager
from typing import Any
from copy import deepcopy
from prepnet.executor.executor import Executor

class FunctionalContext:
    """Functional style preprocess

    Examples:
        >>> context = FunctionalContext()
        >>> # Registering preprocesses
        >>> with context.enter() as f:
        >>>     df.loc['tom'] = f.ordinal(df['tom'])
        >>>     df.loc['becky'] = f.quantile(df['becky'], percentile=0.99)
        >>> with context.enter('secondaly') as f:
        >>>     df.loc['becky'] = f.standardize(df.loc['becky'])
        >>> context.encode(other_df) # Registered preprocesses will be applied.
        >>> context.disable('secondaly').encode(alt_df) # Standardize will not be applied.
        >>> # Decoding preprocess can be same...
    """
    def __init__(self):
        self.stage_preprocess = []
        self.current_stage = {}
        self.stage_index = 0

    @contextmanager
    def enter(self, stage:Any=None)->FunctionalContext:
        if stage is None:
            stage = self.stage_index
            self.stage_index += 1
        old_stage = self.current_stage 
        
        try:
            yield self
        finally:
            self.current_stage = old_stage

    def encode(self, df: pd.DataFrame, executor:Executor=None):
        if executor is None:
            executor = Executor()
        for stage in self.stage_preprocess:
            df = stage.encode(df, executor=executor)
        return df

    def decode(self, df: pd.DataFrame):
        if executor is None:
            executor = Executor()
        for stage in reversed(self.stage_preprocess):
            df = stage.decode(df, executor=executor)
        return df

    # TODO: Implementation of the function style preprocesses.