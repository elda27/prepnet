from collections import OrderedDict

import pytest
import pandas as pd
import numpy as np
from datetime import timedelta

from prepnet.time.daterange_to_index import DateRangeToIndex
from prepnet.time.datefreq_to_index import DateFreqToIndex

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

    converter = DateRangeToIndex(
        input_series.min(), 
        input_series.max() + timedelta(days=2), 
        freq='2D'
    )
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(
        expected_series.astype(np.int32), 
        output_series.astype(np.int32)
    )

    reconstruct_series = converter.decode(output_series)
    pd.testing.assert_series_equal(
        reconstruct_series, 
        input_series
    )
