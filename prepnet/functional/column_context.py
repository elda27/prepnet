from typing import Dict, List

from prepnet.core.module import copydoc
from prepnet.functional.configuration_context_base import ConfigurationContextBase

from prepnet.functional.function_configuration import FunctionConfiguration

from prepnet.core.lambda_converter import LambdaConverter
from prepnet.category.onehot_converter import OnehotConverter
from prepnet.category.ordinal_converter import OrdinalConverter
from prepnet.normalize.quantile_normalize import QuantileNormalize
from prepnet.normalize.standardize import Standardize
from prepnet.math.exp_transform import ExpTransform
from prepnet.math.log_transform import LogTransform
from prepnet.impute.fill_na import FillNA

class ColumnContext(ConfigurationContextBase):
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

    @copydoc(FillNA)
    def fill_na(self, value=0.0, by=None):
        self.add_config(FillNA, value, by)
        return self

    @copydoc(LambdaConverter)
    def convert_lambda(self, encode, decode):
        self.add_config(LambdaConverter, encode, decode)
        return self

    @copydoc(ExpTransform)
    def exp(self):
        self.add_config(ExpTransform)
        return self

    @copydoc(LogTransform)
    def exp(self, interception: float=1.0):
        self.add_config(LogTransform, interception)
        return self

