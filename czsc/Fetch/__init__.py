from .base import freq_map

from czsc.Fetch import tdx, mongo


def use(package):
    if package in ['tdx', ]:
        return tdx
    elif package in ['mongo']:
        return mongo
    else:
        return None


def fetch_future_day(package, code, start=None, end=None):
    engine = use(package)
    if package in ['tdx', 'mongo']:
        return engine.fetch_future_day(code, start, end)
    else:
        return 'Unsupported packages'
