from collections import OrderedDict

import pytest
import pandas as pd
import numpy as np

from prepnet.category.ordinal_converter import OrdinalConverter
from prepnet.category.onehot_converter import OnehotConverter


def test_ordinal_converter():
    input_series = pd.Series(['frame', 'frame', 'old', 'test', 'old', 'frame'])
    expected_series = pd.Series([0, 0, 1, 2, 1, 0])

    converter = OrdinalConverter()
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(expected_series, output_series)

    reconstruct_series = converter.decode(output_series)
    pd.testing.assert_series_equal(reconstruct_series, input_series)

def test_ordinal_converter_same_object():
    input_df = pd.DataFrame({
        'col1': ['one', 'one', 'two', 'three', 'three', 'one'],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
    })
    expected_df = pd.DataFrame({
        'col1': [1, 1, 0, 2, 2, 1],
        'col2': [0, 1, 0, 2, 1, 2],
    })

    converter = OrdinalConverter()
    converters = OrderedDict()
    converters['col2'] = converter
    converters['col1'] = converter

    for col, converter in converters.items():
        output_series = converter.encode(input_df[col])
        pd.testing.assert_series_equal(
            expected_df[col], output_series)
        
        reconstruct_series = converter.decode(output_series)
        pd.testing.assert_series_equal(
            reconstruct_series, input_df[col])

def test_onehot_converter():
    input_df = pd.DataFrame({
        'col1': ['one', 'one', 'two', 'three', 'three', 'one'],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
    })
    expected_df = pd.DataFrame({
        'col1_one': [1, 1, 0, 0, 0, 1],
        'col1_two': [0, 0, 1, 0, 0, 0],
        'col1_three': [0, 0, 0, 1, 1, 0],
        'col2_one': [0, 1, 0, 0, 1, 0],
        'col2_two': [1, 0, 1, 0, 0, 0],
        'col2_three': [0, 0, 0, 1, 0, 1],
    }, dtype=np.uint8)

    converter = OnehotConverter()
    output_df = converter.encode(input_df)
    pd.testing.assert_frame_equal(expected_df, output_df[expected_df.columns])

    reconstruct_df = converter.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)

