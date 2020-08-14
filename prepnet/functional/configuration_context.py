from typing import Dict, List

from prepnet.core.module import copydoc
from prepnet.functional.frame_context import FrameContext

from prepnet.functional.function_configuration import FunctionConfiguration
from prepnet.category.onehot_converter import OnehotConverter
from prepnet.category.ordinal_converter import OrdinalConverter
from prepnet.normalize.quantile_normalize import QuantileNormalize
from prepnet.normalize.standardize import Standardize
from prepnet.impute.nan_imputer import NanImputer

class ConfigurationContext(FrameContext):
    @copydoc(OnehotConverter)
    def onehot(self):
        self.add_config(OnehotConverter)
        return self

    @copydoc(OrdinalConverter)
    def ordinal(self):
        self.add_config(OrdinalConverter)
        return self

    @copydoc(Standardize)
    def standardize(self):
        self.add_config(Standardize)
        return self

    @copydoc(QuantileNormalize)
    def quantile_normalize(self, percentile:float=0.99):
        self.add_config(QuantileNormalize, percentile)
        return self

    @copydoc(NanImputer)
    def nan_impute(self):
        self.add_config(NanImputer)
        return self