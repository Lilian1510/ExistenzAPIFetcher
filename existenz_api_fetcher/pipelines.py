from functools import reduce
from typing import Union
import pandas as pd


def preprocess(df: pd.DataFrame) -> Union[None, pd.DataFrame]:
    """
    Batch processes a DataFrame to make it easier to use.
    Args: DataFrame
    Return: Processed Dataframe
    """
    if df.empty:
        return None
    else:
        df.drop(['result', 'table', '_start', '_stop', '_field', '_measurement', 'loc'], axis=1, inplace=True)
        df.set_index('_time', inplace=True)  # Set datetime as DataFrame index
        df.index.names = ['DateTime']
        df['_value'] = df['_value'].interpolate(method="time")  # Interpolate missing data
        df.index = df.index.to_series().apply(lambda x: x.date()) # Removes unnecessary time
        # Converting the index as date
        df.index = pd.to_datetime(df.index)
        return df


def compute(df: pd.DataFrame) -> Union[None, pd.DataFrame]:
    """
    Generates a dataframe from an existing one.
    Args: DataFrame
    Return: New Dataframe
    """
    if df is None:
        return None
    else:
        new_df = df.filter(['_value'], axis=1)
        new_df.index = df.index
        new_df['_field'] = 'pet'
        new_df = new_df[["_value"]]
        return new_df


def merge(*args) -> Union[None, pd.DataFrame]:
    """
    Function to merge DataFrames.
    *Args: Dataframes
    Returns: DataFrame with DateTime index and all parameters as columns
    """
    if len(args) == 1:
        return args[0]
    else:
        df = reduce(lambda left, right: pd.merge(left, right, on=['DateTime'],
                                                 how='outer'), [d for d in args if d is not None])
        # Interpolate missing values
        df = df.astype(float).interpolate(method="time", limit_direction="both")
        return df
