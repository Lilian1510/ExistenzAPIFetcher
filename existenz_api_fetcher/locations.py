import pandas as pd

"""
Function to geolocate MeteoSwiss or FOEN hydrology stations.
Args: station code (str)
Returns: list with latitude, longitude, elevation (x, y, z) for MeteoSwiss and latitude, longitude (x, y) for FOEN
"""


def geolocate(station: str) -> list:
    meteo_df = pd.read_csv('https://api-datasette.konzept.space/existenz-api/smn_locations.csv?_size=max')
    hydro_df = pd.read_csv('https://api-datasette.konzept.space/existenz-api/hydro_locations.csv?_size=max')

    if station in meteo_df['code'].to_numpy():
        result = meteo_df[meteo_df['code'] == station]
        return [result['lat'].to_numpy()[0], result['lon'].to_numpy()[0], result['alt'].to_numpy()[0]]
    elif station in hydro_df['station_id'].to_numpy():
        result = hydro_df.loc[hydro_df['station_id'] == station]
        return [result['lat'].to_numpy()[0], result['lon'].to_numpy()[0]]
    else:
        print("Please enter a valid station code.")


"""
Function to help find the station code using a map.
Prints two clickable links, one for the MeteoSwiss stations, one for the FOEN ones
"""


def maps():
    print("https://api-datasette.konzept.space/existenz-api/smn_locations")
    print("https://api-datasette.konzept.space/existenz-api/hydro_locations")
