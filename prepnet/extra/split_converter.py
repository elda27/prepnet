from typing import List
import pandas as pd
import numpy as np
from prepnet.core.frame_converter_base import FrameConverterBase

class SplitConverter(FrameConverterBase):
    def __init__(self, n_split, shuffle=True):
        """Split dataframe

        Args:
            n_split ([type]): [description]
            shuffle (bool, optional): [description]. Defaults to True.
        """
        super().__init__()
        self.n_split = n_split
        self.shuffle = shuffle
        self.original_index = None

    def encode(self, df:pd.DataFrame):
        if self.shuffle:
            self.original_index = df.index
            shuffled_index = np.random.shuffle(df.index)
            df = df.loc[shuffled_index]
        return df

    def decode(self, df:List[pd.DataFrame]):
        df = pd.concat(df, axis=0)
        if self.original_index is not None:
            df = df.loc[self.original_index]
        return df
