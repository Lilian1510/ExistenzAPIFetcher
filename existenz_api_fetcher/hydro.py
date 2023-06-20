import warnings
from typing import Union
import pandas as pd
from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction

from existenz_api_fetcher import pipelines

warnings.simplefilter("ignore", MissingPivotFunction)

# Influx DB client configuration
url = "https://influx.konzept.space/"
org = "api.existenz.ch"
bucket = "existenzApi"
token = "0yLbh-D7RMe1sX1iIudFel8CcqCI8sVfuRTaliUp56MgE6kub8-nSd05_EJ4zTTKt0lUzw8zcO73zL9QhC3jtA=="
client = InfluxDBClient(url=url, token=token, org=org)
query_api = client.query_api()


def flow(station: str) -> Union[None, pd.DataFrame]:
    """
    Returns 2 years of streamflow [m3/s] data in the form of a pandas dataframe. Note that not all stations measure streamflow.
    Args: FOEN station code (str). Use the locations.maps() function to open a map with all the stations for more info
    Returns: DataFrame with a datetime index
    """
    flow_df = query_api.query_data_frame('from(bucket:"existenzApi") '
                                         '|> range(start: -2y)'
                                         '|> filter(fn: (r) => r["_measurement"] == "hydro")'
                                         f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                         '|> filter(fn: (r) => r["_field"] == "flow")'
                                         '|> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'
                                         '|> yield(name: "mean")'
                                         )
    flow_df = pipelines.preprocess(flow_df)
    return flow_df


def temperature(station: str) -> Union[None, pd.DataFrame]:
    """
    Returns 2 years of water temperature [°C] data in the form of a pandas dataframe. Note that not all stations measure water temperature.
    Args: FOEN station code (str). Use the locations.maps() function to open a map with all the stations for more info
    Returns: DataFrame with a datetime index
    """
    t_df = query_api.query_data_frame('from(bucket:"existenzApi") '
                                      '|> range(start: -2y)'
                                      '|> filter(fn: (r) => r["_measurement"] == "hydro")'
                                      f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                      '|> filter(fn: (r) => r["_field"] == "temperature")'
                                      '|> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'
                                      '|> yield(name: "mean")'
                                      )
    if not t_df.empty:
        t_df = pipelines.preprocess(t_df)
        return t_df
    else:
        print(f"Station {station} does not measure water temperature...")
        return None


def height(station: str) -> Union[None, pd.DataFrame]:
    """
    Returns 2 years of River or lake height [m over sea level] data in the form of a pandas dataframe.
    Args: FOEN station code (str). Use the locations.maps() function to open a map with all the stations for more info
    Returns: DataFrame with a datetime index
    """
    h_df = query_api.query_data_frame('from(bucket:"existenzApi") '
                                      '|> range(start: -2y)'
                                      '|> filter(fn: (r) => r["_measurement"] == "hydro")'
                                      f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                      '|> filter(fn: (r) => r["_field"] == "height")'
                                      '|> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'
                                      '|> yield(name: "mean")'
                                      )
    h_df = pipelines.preprocess(h_df)
    return h_df
