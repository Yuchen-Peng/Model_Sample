# -*- coding: utf-8 -*-
from typing import List

import numpy as np
import pandas as pd
from pandas.tseries.offsets import DateOffset
import datetime


def create_target_dict(
  conf,
  cutoff_date: datetime.datetime,
  group_column: List[str],
) -> dict:
  '''
  Create target for any given cutoff_date, at given TARGET_DURATION (length of timewindow for target to mature)
  '''
  targets = dict(
        group_column=group_column,
        add_columns=[],
        grouped_features=[
            {
                "name": "target",
                "source_col": "sales_quantity",
                "window": {"months": conf.TARGET_DURATION},
                "snap_date": cutoff_date + DateOffset(months=conf.TARGET_DURATION),
                "func": sum,
            },
        ] 
    )
    return targets


def create_base_features_dict(
    conf,
    cutoff_date: datetime.datetime,
    group_column: List[str],
) -> dict:
    '''
    Create base features, excluding target
    '''
  base_features = dict(
        group_column=group_column,
        add_columns=[],
        grouped_features=[
            {
                "name": "f_order_quantity_2mths",
                "source_col": "sales_order_item_quantity",
                "window": {"months": 2},
                "func": sum,
                "snap_date": cutoff_date,
            },
        ] 
    )
    return base_features


def create_macro_features_dict(
    conf,
    cutoff_date: datetime.datetime,
    group_column: List[str],
    ) -> dict:
        macro_features = dict(
            group_column=group_column,
            add_columns=[],
            grouped_features=[
                {
                    "name": "f_order_quantity_3mths",
                    "source_col": "sales_order_item_quantity",
                    "window": {"months": 3},
                    "func": sum,
                    "snap_date": cutoff_date,
                },
            ] 
        )
        return macro_features
