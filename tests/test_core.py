import pytest
import pandas as pd
import numpy as np

from prepnet.executor.executor import Executor

from prepnet.category.onehot_converter import OnehotConverter
from prepnet.core.frame_converter_context import FrameConverterContext


def test_executor():
    input_df = pd.DataFrame({
        'col1': ['one', 'one', 'two', 'three', 'three', 'one'],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
    })
    expected_df = pd.DataFrame({
        'col1': [1, 1, 0, 2, 2, 1],
        'col2': [0, 1, 0, 2, 1, 2],
    })
    expected_series = pd.Series([0, 0, 1, 2, 1, 0])

    

    executor = Executor()
    executor.encode()

    converter = OrdinalConverter()
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(expected_series, output_series)

    reconstruct_series = converter.decode(output_series)
    pd.testing.assert_series_equal(reconstruct_series, input_series)


def test_frame_converter_context():
    input_df = pd.DataFrame({
        'col1': ['one', 'one', 'two', 'three', 'three', 'one'],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
    })
    expected_df = pd.DataFrame({
        'col1_one': [1, 1, 0, 0, 0, 1],
        'col1_two': [0, 0, 1, 0, 0, 0],
        'col1_three': [0, 0, 0, 1, 1, 0],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
    }, dtype=np.uint8)
    converter = FrameConverterContext(OnehotConverter())
    
    pd.testing.assert_frame_equal(expected_df, output_df[expected_df.columns])

    reconstruct_df = converter.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)
