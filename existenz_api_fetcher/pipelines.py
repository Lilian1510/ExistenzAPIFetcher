from functools import reduce

import pandas as pd


def preprocess(df):
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
        return df


def compute(df):
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


def merge(*args):
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
