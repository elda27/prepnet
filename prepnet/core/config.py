from contextlib import contextmanager

config = {
    'keep_original': True,
    'raise_satisfied': True,
}

@contextmanager
def config_context(key, value):
    if key not in config:
        raise KeyError(key)
    old = config[key]
    config[key] = value
    yield
    config[key] = old

@contextmanager
def set_config(key, value):
    if key not in config:
        raise KeyError(key)
    config[key] = value

def get_config(key):
    return config[key]
