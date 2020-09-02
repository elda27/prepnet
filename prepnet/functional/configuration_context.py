from typing import Dict, List

from prepnet.core.module import copydoc
from prepnet.functional.frame_context import FrameContext

from prepnet.functional.function_configuration import FunctionConfiguration
from prepnet.functional.column_context import ColumnContext

from prepnet.functional.pandas_accessor import PandasAccessor

class ConfigurationContext(FrameContext, ColumnContext):
    # @property
    # def dt(self):
    #     return PandasAccessor(self, 'dt')
    # @property
    # def str(self):
    #     return PandasAccessor(self, 'str')
    # @property
    # def cat(self):
    #     return PandasAccessor(self, 'cat')
    pass
