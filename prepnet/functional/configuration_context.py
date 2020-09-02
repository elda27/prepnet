from typing import Dict, List

from prepnet.core.module import copydoc
from prepnet.functional.frame_context import FrameContext

from prepnet.functional.function_configuration import FunctionConfiguration
from prepnet.functional.column_context import ColumnContext

from prepnet.core.pandas_accessor import PandasAccessor

class ConfigurationContext(FrameContext, ColumnContext):
    dt = PandasAccessor('dt')
    str = PandasAccessor('str')
    cat = PandasAccessor('cat')
