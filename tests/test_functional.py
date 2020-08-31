from collections import OrderedDict

import pytest
import pandas as pd
import numpy as np

from prepnet.executor.executor import Executor

from prepnet.functional.functional_context import FunctionalContext


def test_functional_context():
    input_df = pd.DataFrame({
        'col1': ['one', 'one', 'two', 'three', 'three', 'one'],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
        'col3': ['two', 'one', 'three', 'one', 'two', 'two'],
    })
    expected_df = pd.DataFrame({
        'col1_one': [1, 1, 0, 0, 0, 1],
        'col1_two': [0, 0, 1, 0, 0, 0],
        'col1_three': [0, 0, 0, 1, 1, 0],
        'col2_one': [0, 1, 0, 0, 1, 0],
        'col2_two': [1, 0, 1, 0, 0, 0],
        'col2_three': [0, 0, 0, 1, 0, 1],
        'col3': [0, 1, 2, 1, 0, 0],
    }, dtype=np.uint8)
    
    context = FunctionalContext()
    with context.enter():
        context['col1', 'col2'].onehot()
        context['col3'].ordinal()

    output_df = context.encode(input_df)
    pd.testing.assert_frame_equal(
        expected_df.astype(np.uint8), 
        output_df[expected_df.columns].astype(np.uint8)
    )

    reconstruct_df = context.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)


def test_disable_execution():
    input_df = pd.DataFrame({
        'col': [1, 2, 3, 4, 5, 6],
    })
    expected_original_df = pd.DataFrame({
        'col': [2, 3, 4, 5, 6, 7],
    })
    expected_result_df = pd.DataFrame({
        'col': [3, 4, 5, 6, 7, 8],
    })

    plus = lambda xs: xs + 1
    minus = lambda xs: xs - 1

    context = FunctionalContext()
    with context.enter():
        context['col'].convert_lambda(
            plus, minus
        )
    with context.enter('minus'):
        context['col'].convert_lambda(
            minus, plus
        )
    with context.enter():
        context['col'].convert_lambda(
            plus, minus
        )
    original_output_df = context.encode(input_df)
    pd.testing.assert_frame_equal(
        original_output_df[input_df.columns], expected_original_df
    )
    original_reconstruct_df = context.decode(original_output_df)
    pd.testing.assert_frame_equal(
        original_reconstruct_df[input_df.columns], input_df
    )
    
    disabled_context = context.disable('minus')
    result_output_df = disabled_context.encode(input_df)
    pd.testing.assert_frame_equal(
        result_output_df[expected_result_df.columns], 
        expected_result_df
    )
    result_reconstruct_df = disabled_context.decode(result_output_df)
    pd.testing.assert_frame_equal(
        result_reconstruct_df[input_df.columns], 
        input_df
    )

def test_context_all_columns():
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
    
    context = FunctionalContext()
    with context.enter():
        context.onehot()

    output_df = context.encode(input_df)
    pd.testing.assert_frame_equal(
        expected_df.astype(np.uint8), 
        output_df[expected_df.columns].astype(np.uint8)
    )

    reconstruct_df = context.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)
