from collections import OrderedDict

import pytest
import pandas as pd
import numpy as np
from datetime import timedelta

from prepnet.time.frequency_time import FrequencyTimeConverter

def test_ordinal_converter():
    input_series = pd.to_datetime(pd.Series([
        '2020-09-01',
        '2020-09-04',
        '2020-09-13',
        '2020-09-07',
        '2020-09-04',
        '2020-09-01',
    ]))
    expected_series = pd.Series([0, 1, 6, 3, 1, 0])

    converter = FrequencyTimeConverter(
        input_series.min(), 
        input_series.max() + timedelta(days=2), 
        freq='2D'
    )
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(expected_series, output_series)

    reconstruct_series = converter.decode(output_series)
    pd.testing.assert_series_equal(reconstruct_series, input_series)
