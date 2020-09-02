import pandas as pd

class PandasAcessorWrapper:
    def __init__(self, accessor:str, method:str):
        self.method = method
        self.accessor = accessor
        self.__call__.__doc__ = self.get_method().__doc__

    def get_method(self):
        return getattr(getattr(pd.DataFrame, accessor), method)

    def __call__(self, *args, **kwargs):
        return self.get_method()(*args, **kwargs)

class PandasAccessor:
    """Preprocess by pandas accessor such as "dt", "str" and so on.
    """
    def __init__(self, accessor: str):
        """Accessor name of string

        Args:
            accessor (str): Accessor name
        """
        self.accessor = accessor

    def __getattr__(self, name):
        return PandasAcessorWrapper(self.accessor, self.name)
