from prepnet.core.frame_converter_base import FrameConverterBase
from prepnet.executor.executor import Executor

class Stage(FrameConverterBase):
    def __init__(self, stage_key):
        self.stage_key = stage_key
        self.enable = True
        self.converters = []

    def encode(self, df: pd.DataFrame, executor=None):
        if not self.enable:
            return df
        if executor is None:
            executor = Executor(self.converters)
        return executor.encode(df)
        
    def decode(self, df: pd.DataFrame, executor=None):
        if not self.enable:
            return df
        if executor is None:
            executor = Executor(self.converters)
        return executor.decode(df)