# coding: utf-8
import json
import pandas as pd
import numpy as np


class DataEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, pd.Timestamp):
            return obj.strftime("%Y-%m-%d")
        elif isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return json.JSONEncoder.default(self, obj)