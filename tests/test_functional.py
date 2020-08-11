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
        c1 = context['col1', 'col2'].onehot()
        c2 = context['col3'].ordinal()

    output_df = context.encode(input_df)
    pd.testing.assert_frame_equal(expected_df, output_df[expected_df.columns])

    reconstruct_df = context.decode(output_df)
    pd.testing.assert_frame_equal(reconstruct_df[input_df.columns], input_df)
