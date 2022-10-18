import warnings

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

"""
Functions to fetch hydrological data from the Existenz API InfluxDB database. The last 2 years of data are available.
Args: FOEN station code (str). Use the locations.map() function to open a map with all the stations for more info
Returns: DataFrame with a datetime index
"""


def flow(station: str) -> object:
    # Streamflow [m3/s]
    flow_df = query_api.query_data_frame('from(bucket:"existenzApi") '
                                         '|> range(start: -2y)'
                                         '|> filter(fn: (r) => r["_measurement"] == "hydro")'
                                         f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                         '|> filter(fn: (r) => r["_field"] == "flow")'
                                         '|> aggregateWindow(every: 1d, fn: mean, createEmpty: false)'
                                         '|> yield(name: "mean")'
                                         )
    print(type(flow_df))
    flow_df = pipelines.preprocess(flow_df)
    return flow_df


"""
Note that not all stations measure surface water temperature.
"""


def temperature(station: str) -> object:
    # Water temperature [°C]
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


def height(station: str) -> object:
    # River or lake height [m over sea level]
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
