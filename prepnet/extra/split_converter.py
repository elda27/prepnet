from prepnet.core.frame_converter_base import FrameConverterBase

class SplitConverter(FrameConverterBase):
    """Split n-th elements
    Now this class didn't support FrameConverterContext
    """
    def __init__(self):
        super().__init__()
