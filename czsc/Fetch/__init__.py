from .base import freq_map

from czsc.Fetch import tdx, mongo


def use(package):
    if package in ['tdx', ]:
        return tdx
    elif package in ['mongo']:
        return mongo
    else:
        return None


def get_bar(package, code, start=None, end=None, freq='day', exchange=None):
    engine = use(package)
    if package in ['tdx']:
        return engine.get_bar(code, start, end, freq=freq, exchange=None)
    else:
        return 'Unsupported packages'
