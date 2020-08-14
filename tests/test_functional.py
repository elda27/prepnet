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
    with context.enter():
        context['col3'].ordinal()

    output_df = context.encode(input_df)
    pd.testing.assert_frame_equal(expected_df, output_df[expected_df.columns].astype(np.uint8))

    reconstruct_df = context.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)


def test_post_processor():
    input_df = pd.DataFrame({
        'col1': ['one', 'one', 'two', 'three', 'three', 'one'],
        'col2': ['two', 'one', 'two', 'three', 'one', 'three'],
        'col3': ['two', 'one', 'three', 'one', 'two', 'two'],
    })
    expected_df_list = [
        pd.DataFrame({
            'col1': ['one', 'one', 'two'],
            'col2': ['two', 'one', 'two'],
            'col3': ['two', 'one', 'three'],
        }),
        pd.DataFrame({
            'col1': ['three', 'three', 'one'],
            'col2': ['three', 'one', 'three'],
            'col3': ['one', 'two', 'two'],
        }, index=pd.RangeIndex(start=3, stop=6, step=1))
    ]
    context = FunctionalContext()
    context.post.split(2, shuffle=False)
    output_df_list = context.encode(input_df)
    for o, e in zip(expected_df_list, output_df_list):
        pd.testing.assert_frame_equal(o, e)
    reconstruct_df = context.decode(output_df_list)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)

