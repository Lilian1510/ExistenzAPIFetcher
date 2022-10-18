import warnings

from influxdb_client import InfluxDBClient
from influxdb_client.client.warnings import MissingPivotFunction
from pyet import penman, pm, pm_fao56

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
Functions to fetch weather data from the Existenz API InfluxDB database. The last 2 years of data are available.
Args: MeteoSwiss station code (str). Use the locations.map() function to open a map with all the stations for more info
Returns: DataFrame with a datetime index
"""


def rainfall(station: str):
    # Rainfall [mm/day]
    rr_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                       '|> range(start: -2y)'
                                       '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                       f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                       '|> filter(fn: (r) => r["_field"] == "rr")'
                                       '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                       '|> yield(name: "mean")'
                                       )
    rr_df = pipelines.preprocess(rr_df)
    return rr_df


def temperature(station: str):
    # Mean temperature [°C]
    tt_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                       '|> range(start: -2y)'
                                       '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                       f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                       '|> filter(fn: (r) => r["_field"] == "tt")'
                                       '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                       '|> yield(name: "mean")'
                                       )
    tt_df = pipelines.preprocess(tt_df)
    return tt_df


def min_temperature(station: str):
    # Minimum temperature[°C]
    mintt_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                          '|> range(start: -2y)'
                                          '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                          f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                          '|> filter(fn: (r) => r["_field"] == "tt")'
                                          '|> aggregateWindow(every: 1d, fn: min, createEmpty: true)'
                                          '|> yield(name: "mean")'
                                          )
    mintt_df = pipelines.preprocess(mintt_df)
    return mintt_df


def max_temperature(station: str):
    # Maximum temperature[°C]
    maxtt_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                          '|> range(start: -2y)'
                                          '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                          f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                          '|> filter(fn: (r) => r["_field"] == "tt")'
                                          '|> aggregateWindow(every: 1d, fn: max, createEmpty: true)'
                                          '|> yield(name: "mean")'
                                          )
    maxtt_df = pipelines.preprocess(maxtt_df)
    return maxtt_df


def humidity(station: str):
    # Relative humidity [%]
    rh_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                       '|> range(start: -2y)'
                                       '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                       f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                       '|> filter(fn: (r) => r["_field"] == "rh")'
                                       '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                       '|> yield(name: "mean")')
    rh_df = pipelines.preprocess(rh_df)
    return rh_df


def wind_speed(station: str):
    # Wind speed [km/h]
    ff_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                       '|> range(start: -2y) '
                                       '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                       f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                       '|> filter(fn: (r) => r["_field"] == "ff")'
                                       '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                       '|> yield(name: "mean")'
                                       )
    ff_df = pipelines.preprocess(ff_df)
    return ff_df


def radiation(station: str):
    # Radiation intensity [W/m2]
    rad_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                        '|> range(start: -2y) '
                                        '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                        f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                        '|> filter(fn: (r) => r["_field"] == "rad")'
                                        '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                        '|> yield(name: "mean")'
                                        )
    rad_df = pipelines.preprocess(rad_df)
    return rad_df


def pressure(station: str):
    # Pressure at station level [hPa]
    qfe_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                        '|> range(start: -2y) '
                                        '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                        f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                        '|> filter(fn: (r) => r["_field"] == "qfe")'
                                        '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                        '|> yield(name: "mean")'
                                        )
    qfe_df = pipelines.preprocess(qfe_df)
    return qfe_df


def sunshine_duration(station: str):
    # Sunshine duration [min]
    ss_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                       '|> range(start: -2y) '
                                       '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                       f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                       '|> filter(fn: (r) => r["_field"] == "ss")'
                                       '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                       '|> yield(name: "mean")'
                                       )
    ss_df = pipelines.preprocess(ss_df)
    return ss_df


def dew_point(station: str):
    # Dew point 2 m above ground
    td_df = query_api.query_data_frame('from(bucket: "existenzApi")'
                                       '|> range(start: -2y) '
                                       '|> filter(fn: (r) => r["_measurement"] == "smn")'
                                       f'|> filter(fn: (r) => r["loc"] == "{station}")'
                                       '|> filter(fn: (r) => r["_field"] == "td")'
                                       '|> aggregateWindow(every: 1d, fn: mean, createEmpty: true)'
                                       '|> yield(name: "mean")'
                                       )
    td_df = pipelines.preprocess(td_df)
    return td_df


"""
Potential evapotranspiration functions need the station elevation and latitude to compute evapotranspiration.
Please see the locations.find() function for more info
"""


def ev_penman(station: str, elevation: int, lat: float):
    penman_df = pipelines.compute(radiation(station))
    if penman_df is None:
        return None
    else:
        # Compute potential evaporation with Penman equation
        ev = penman(tmean=temperature(station)['_value'], wind=wind_speed(station)['_value'],
                    rs=radiation(station)['_value'], elevation=elevation,
                    lat=lat, tmax=max_temperature(station)['_value'], tmin=min_temperature(station)['_value'],
                    rh=humidity(station)['_value'])
        # Interpolate missing data
        penman_df['_value'] = ev.interpolate()
        return penman_df


def ev_penman_monteith(station: str, elevation: int, lat: float):
    pm_df = pipelines.compute(radiation(station))
    if pm_df is None:
        return None
    else:
        # Compute potential evaporation with Penman-Monteith equation
        ev = pm(tmean=temperature(station)['_value'], wind=wind_speed(station)['_value'],
                rs=radiation(station)['_value'], elevation=elevation,
                lat=lat, tmax=max_temperature(station)['_value'], tmin=min_temperature(station)['_value'],
                rh=humidity(station)['_value'])
        # Interpolate missing data
        pm_df['_value'] = ev.interpolate()
        return pm_df


def ev_fao56(station: str, elevation: int, lat: float):
    fao_df = pipelines.compute(radiation(station))
    if fao_df is None:
        return None
    else:
        # Compute potential evaporation with Fao56 equation
        ev = pm_fao56(tmean=temperature(station)['_value'], wind=wind_speed(station)['_value'],
                      rs=radiation(station)['_value'], elevation=elevation,
                      lat=lat, tmax=max_temperature(station)['_value'], tmin=min_temperature(station)['_value'],
                      rh=humidity(station)['_value'])
        # Interpolate missing data
        fao_df['_value'] = ev.interpolate()
        return fao_df
