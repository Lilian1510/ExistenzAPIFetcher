from functools import reduce

import pandas as pd

"""
Functions designed to batch-process a DataFrame to make it easier to use.
Args: DataFrame
Return: Processed Dataframe
"""


def preprocess(df):
    if df.empty:
        return None
    else:
        df.drop(['result', 'table', '_start', '_stop', '_field', '_measurement', 'loc'], axis=1, inplace=True)
        df.set_index('_time', inplace=True)  # Set datetime as DataFrame index
        df.index.names = ['DateTime']
        df['_value'] = df['_value'].interpolate(method="time")  # Interpolate missing data
        return df


def compute(df):
    if df is None:
        return None
    else:
        new_df = df.filter(['_value'], axis=1)
        new_df.index = df.index
        new_df['_field'] = 'pet'
        new_df = new_df[["_value"]]
        return new_df


"""
Function to merge DataFrames.
*Args: Dataframes
Returns: DataFrame with DateTime index and all parameters as columns
"""


def merge(*args):
    if len(args) == 1:
        return args[0]
    else:
        df = reduce(lambda left, right: pd.merge(left, right, on=['DateTime'],
                                                 how='outer'), [d for d in args if d is not None])
        # Interpolate missing values
        df = df.astype(float).interpolate(method="time", limit_direction="both")
        return df
