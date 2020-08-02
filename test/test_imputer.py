import pytest
import pandas as pd
import numpy as np

from prepnet.impute.nan_imputer import NanImputer
from prepnet.impute.drop_na import DropNA


@pytest.mark.parametrize(('input', 'expected', 'impute_value'), [
     
])
def test_nan_imputer(input, expected):
    NanImputer()
    assert is_prime(input) == expected