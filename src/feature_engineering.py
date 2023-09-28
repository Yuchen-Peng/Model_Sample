# -*- coding: utf-8 -*-
import pandas as pd
import re
import datetime
from typing import List
from joblib import Parallel, delayed

from utils import log

from features_config import (
    create_base_features_dict,
    create_macro_features_dict,
    create_target_dict,
)


def create_a_column(df: pd.DataFrame, definition: dict) -> pd.Series:
    """Add a column to the raw data based on definition

    Args:
        df: Raw data DataFrame
        definition: A dict defining the new column, with following params
          name (str): Name of new column
          func (function): Function to `apply` to the DataFrame

    Returns:
       pd.Series of feature
    """
    log.info(f"Adding column {definition['name']}")
    return df.assign(**{definition["name"]: definition["func"]})[definition["name"]]


def create_a_grouped_feature(
    df: pd.DataFrame, definition: dict, group_col: str
) -> pd.Series:
    """Create one feature from raw data in `df` following `definition` grouped
    by group_col

    Args:
        df: Raw data DataFrame
        definition: A dict defining a feature, with following params
          name (str): Name of the new feature
          source_col (str): Name(s) of column to aggregate
          func (function): Aggregation function
          window (dict): (Optional) how far to look back for this calculation, by default use all data up to snap_date
          snap_date (datetime): (Optional) (Historical) date up to which to calculate the feature -
            throw away later raw data
        group_col: Column to group on, usually sold_to_cust_name

    Returns:
       pd.Series of feature with group_col as index
    """
    log.info(f"Creating feature {definition['name']}")

    cutoff = definition.get("snap_date")
    df_cut = df[df.order_date < cutoff]

    window = definition.get("window", None)
    if window:
        df_cut = df_cut[
            df_cut.order_date >= cutoff - pd.tseries.offsets.DateOffset(**window)
        ]

    g = df_cut.groupby(group_col).agg(
        **{definition["name"]: (definition["source_col"], definition["func"])}
    )
    return g


def return_feature_columns(df:pd.DataFrame,
                           feature_dict: dict,
                           ):
    """take a dataframe and apply a dictionary for feature generation
    generate an order_date column & yr/mth/wk/day of week(dow) column set
    using create_a_grouped_feature() generate a dictionary of columns -
        based off of the passed feature dictionary
    Args:
        df: Raw data DataFrame
        feature_dict: A dict defining a feature, with following params
          name (str): Name of the new feature
          source_col (str): Name(s) of column to aggregate
          func (function): Aggregation function
          window (dict): (Optional) how far to look back for this calculation, by default use all data up to snap_date
          snap_date (datetime): (Historical) date up to which to calculate the feature -
            throw away later raw data
        group_col: Column to group on, usually sold_to_cust_name
    
    Returns:
       pd.DataFrame of feature with group_col as index
    """
    df_copy = df.copy()
    df_copy = prepare_order_dates(df_copy)

    for def_add_col in feature_dict.get("add_columns", []):
        col = create_a_column(df_copy, def_add_col)
        df_copy[def_add_col["name"]] = col
    return_cols = {
        def_group_feat["name"]: create_a_grouped_feature(
            df_copy, def_group_feat, group_col=feature_dict["group_column"]
        )
        for def_group_feat in feature_dict.get("grouped_features", [])
    }

    return pd.concat(return_cols.values(), axis=1)


def generate_base_features(
    conf, df: pd.DataFrame, snap_date: datetime.datetime
) -> pd.DataFrame:
    """create the base, cancelled, & return features from features_dicts
    for one single snap_date

    ARGS:
        conf: global config class
        df: preprocessed dataframe
        snap_date: passes a date, to be used in parallelization

    RETURNS: a single dataframe of all base features for the given snap_date
    """
    dict_return = create_return_features_dict(conf, snap_date, conf.FINAL_GRANULARITY)
    dict_cancel = create_cancellation_features_dict(
        conf, snap_date, conf.FINAL_GRANULARITY
    )
    dict_base = create_base_features_dict(conf, snap_date, conf.FINAL_GRANULARITY)

    cols_returns = return_feature_columns(df[(df["type"] == "Return")], dict_return)

    cols_cancellations = return_feature_columns(
        df[(df["sales_order_item_rejection_status_code"] == "C")], dict_cancel
    )

    cols_base = return_feature_columns(
        df[
            (df["type"] == "Trade")
            & (df["sales_order_item_rejection_status_code"] != "C")
        ],
        dict_base,
    )

    df_out = cols_base.merge(
        cols_cancellations,
        how="left",
        left_index=True,
        right_index=True,
    ).merge(
        cols_returns,
        how="left",
        left_index=True,
        right_index=True,
    )

    df_out["snap_date"] = snap_date

    return df_out


def generate_targets(
    conf, df: pd.DataFrame, snap_date: datetime.datetime
) -> pd.DataFrame:

    """create the target for one single snap_date

    ARGS:
        conf: global config class
        df: preprocessed dataframe
        snap_date: passes a date, to be used in parallelization

    RETURNS: a single dataframe of target columns for the given snap_date
    """
    dict_targets = create_target_dict(conf, snap_date, conf.FINAL_GRANULARITY)

    cols_targets = return_feature_columns(
        df[
            (df["type"] == "Trade")
            & (df["sales_order_item_rejection_status_code"] != "C")
        ],
        dict_targets,
    )

    df_out = cols_targets

    df_out['snap_date'] = snap_date
    # df_out['pred_window_start'] = snap_date
    # df_out['pred_window_end'] = snap_date + relativedelta(months=conf.TARGET_DURATION) - relativedelta(days=1)
        
    return df_out


def generate_macro_features(
    conf,
    df_func: pd.DataFrame,
    group_cols: str,
    snap_date: datetime.datetime,
) -> pd.DataFrame:

    """
    generate list of features described in macro_features_dict
    for the given snap_date and the given group_cols

    ARGS:
        conf: global config class
        df: preprocessed dataframe
        snap_date: passes a date, to be used in parallelization

    RETURNS: a single dataframe of macro features
    for the given snap_date and the given group_cols
    """
    df = df_func.copy()

    dict_macro = create_macro_features_dict(
        conf, snap_date, conf.GROUPBY_COLS_DICT[group_cols]
    )

    # -- remove any missing values from the groupby columns
    df_input = remove_missing_values(conf.GROUPBY_COLS_DICT[group_cols], df)

    cols_macro = return_feature_columns(df_input, dict_macro).reset_index()
    # -- renanme cols to know which features were generated
    # -- may want to create an xref table if names are too unwieldly
    n = len(conf.GROUPBY_COLS_DICT[group_cols])
    cols_macro.columns = list(cols_macro.columns[:n]) + [col+"_"+group_cols for col in cols_macro.columns[n:]]

    cols_macro["snap_date"] = snap_date
    cols_macro["attributes"] = str(conf.GROUPBY_COLS_DICT[group_cols])
    cols_macro["groupby_name"] = str(group_cols)

    return cols_macro
