import os
import datetime
import re
import logging
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta


class GlobalValues:
    def __init__(self):
        self._NA_DATE = pd.to_datetime("1900-01-01")
        self._ROOT_PATH = os.path.dirname(os.path.dirname(os.getcwd()))
        self._DATA_PATH = os.path.join(self._ROOT_PATH, 'data_input')
        self._MODEL_PATH = os.path.join(self._ROOT_PATH, 'models')
        self._HISTORY_START = "2022-08-01"
        self._HISTORY_END = "2023-08-01"
        self._TARGET_DURATION = 3
        self._NUM_MODELS = 2
        self._FINAL_GRANULARITY = ["cust_name", "product_id"]
        self._N_PARALLEL = 3
        self._ATTRIBUTE_LIST = (
            self._FINAL_GRANULARITY + 
            ['market_category',# -- needed for macro feats & dummies
             'market_group',# -- needed for macro feats & dummies
             ]
             )
        self._GROUPBY_COLS_DICT = {
        'cust_market_category':['cust_name', 'market_category'],
        'cust_market_group':['sold_to_cust_name', 'market_group'],
        'market_category':['market_category'],
        'market_group':['market_group'],
        }
        self._DUMMY_COLS_DICT = {
        'f_market_category': 'market_category',
        'f_market_group': 'market_group',
        }

    @property
    def NA_DATE(self):
        return self._NA_DATE

    @property
    def ROOT_PATH(self):
        return self._ROOT_PATH

    @property
    def DATA_PATH(self):
        return self._DATA_PATH

    @property
    def NUM_MODELS(self):
        return self._NUM_MODELS

    @property
    def HISTORY_START(self):
        return self._HISTORY_START

    @property
    def HISTORY_END(self):
        return self._HISTORY_END

    @property
    def HISTORY_WINDOW(self):
        return pd.date_range(
            start=pd.to_datetime(self.HISTORY_START),
            end=pd.to_datetime(self.HISTORY_END),
            freq="MS",
        )

    @property
    def FEATURE_WINDOW(self):
        return pd.date_range(
            start=(
                pd.to_datetime(self.HISTORY_START)
                -
                relativedelta(months=self.TARGET_DURATION*(self.NUM_MODELS - 1))
            ),
            end=pd.to_datetime(self.HISTORY_END),
            freq="MS",
        )

    @property
    def TARGET_DURATION(self):
        return self._TARGET_DURATION

    @property
    def FINAL_GRANULARITY(self):
        return self._FINAL_GRANULARITY

    @property
    def N_PARALLEL(self):
        return self._N_PARALLEL

    @property
    def ATTRIBUTE_LIST(self):
        return self._ATTRIBUTE_LIST

    @property
    def GROUPBY_COLS_DICT(self):
        return self._GROUPBY_COLS_DICT

    @property
    def DUMMY_COLS_DICT(self):
        return self._DUMMY_COLS_DICT

    @property
    def DICT_MAP_VAL(self):
        return self._DICT_MAP_VAL

    def set_history_start(self, value):
        self._HISTORY_START = value

    def set_history_end(self, value):
        self._HISTORY_END = value

    def set_final_granularity(self, value):
        self._FINAL_GRANULARITY = value

    def set_n_parallel(self, value):
        self._N_PARALLEL = value
