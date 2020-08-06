import pytest
import pandas as pd
import numpy as np

from prepnet.impute.nan_imputer import NanImputer
from prepnet.impute.drop_na import DropNA


@pytest.mark.parametrize(('input', 'expected', 'impute_value'), [
    (
        [
            [10, 20, 30],
            [10, np.nan, 30],
            [10, 20, np.nan],
        ],
        [
            [10, 20, 30],
            [10, 10, 30],
            [10, 20, 10],
        ],
        10
    )
])
def test_nan_imputer(input, expected, impute_value):
    input_df = pd.DataFrame(input).astype(float)
    output_df = pd.DataFrame(expected).astype(float)

    imputer = NanImputer(impute_value)
    result_df = imputer.encode(input_df)
    pd.testing.assert_frame_equal(output_df, result_df)
    
    reconstruct_df = imputer.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df, input_df)


@pytest.mark.parametrize(('input', 'expected', 'impute_value'), [
     (
        [
            [10, 20, 30],
            [10, np.nan, 30],
            [10, 20, np.nan],
        ],
        [
            [10, 20, 30],
        ],
        10
    )
])
def test_dropna(input, expected, impute_value):
    input_df = pd.DataFrame(input, dtype=float)
    output_df = pd.DataFrame(expected, dtype=float)

    imputer = DropNA()
    result_df = imputer.encode(input_df)
    pd.testing.assert_frame_equal(output_df, result_df)

    reconstruct_df = imputer.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df, input_df)

