from collections import OrderedDict

import pytest
import pandas as pd
import numpy as np

from prepnet.executor.executor import Executor

from prepnet.core.null_converter import NullConverter
from prepnet.category.ordinal_converter import OrdinalConverter
from prepnet.category.onehot_converter import OnehotConverter
from prepnet.core.frame_converter_context import FrameConverterContext
from prepnet.core.converter_reference import ConverterReference

def test_executor():
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
    converters['col1'] = ConverterReference(converter)

    executor = Executor(converters)
    output_df = executor.encode(input_df)

    pd.testing.assert_frame_equal(expected_df, output_df)

    reconstruct_df = executor.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df, input_df)


def test_partial_convert_using_frame_converter_context():
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
    
    executor = Executor({
        'col1': converter
    })
    output_df = executor.encode(input_df)

    pd.testing.assert_frame_equal(expected_df, output_df[expected_df.columns])

    reconstruct_df = executor.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)


def test_null_converter():
    input_series = pd.Series([1,2,3,4,5,6,7])

    converter = NullConverter()
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(input_series, output_series)

    reconstruct_series = converter.decode(output_series)

    pd.testing.assert_series_equal(input_series, reconstruct_series)
