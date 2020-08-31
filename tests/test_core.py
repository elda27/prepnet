import pytest
import pandas as pd
import numpy as np

from prepnet.executor.executor import Executor

from prepnet.executor.converter_array import ConverterArray
from prepnet.core.null_converter import NullConverter
from prepnet.core.sequence_converter import SequenceConverter
from prepnet.category.ordinal_converter import OrdinalConverter
from prepnet.category.onehot_converter import OnehotConverter

def test_executor():
    input_df = pd.DataFrame({
        'col1': ['one', 'one', 'two', 'three', 'three', 'one'],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
    })
    expected_df = pd.DataFrame({
        'col1': [0, 0, 1, 2, 2, 0],
        'col2': [0, 1, 0, 2, 1, 2],
    })

    converters = ConverterArray(['col1', 'col2'])
    converters.append(OrdinalConverter())

    executor = Executor([converters])
    output_df = executor.encode(input_df)

    pd.testing.assert_frame_equal(expected_df, output_df)

    reconstruct_df = executor.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df, input_df)


def test_null_converter():
    input_series = pd.Series([1,2,3,4,5,6,7])

    converter = NullConverter()
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(input_series, output_series)

    reconstruct_series = converter.decode(output_series)

    pd.testing.assert_series_equal(input_series, reconstruct_series)


def test_sequence_converter():
    input_series = pd.Series([1,2,3,4,5,6,7])

    converter = SequenceConverter([
        NullConverter(),
        NullConverter(),
    ])
    output_series = converter.encode(input_series)
    pd.testing.assert_series_equal(input_series, output_series)

    reconstruct_series = converter.decode(output_series)

    pd.testing.assert_series_equal(input_series, reconstruct_series)

