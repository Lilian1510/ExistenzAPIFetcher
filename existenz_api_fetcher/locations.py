import pandas as pd
import geopandas as gpd


def geolocate(station: str) -> list:
    """
    Function to geolocate MeteoSwiss or FOEN hydrology stations.
    Args: station code (str)
    Returns: list with latitude, longitude, elevation (x, y, z) for MeteoSwiss and latitude, longitude (x, y) for FOEN
    """
    meteo_df = pd.read_csv('https://api-datasette.konzept.space/existenz-api/smn_locations.csv?_size=max')
    hydro_df = pd.read_csv('https://api-datasette.konzept.space/existenz-api/hydro_locations.csv?_size=max')

    if station in meteo_df['code'].to_numpy():
        result = meteo_df[meteo_df['code'] == station]
        return [result['lat'].to_numpy()[0], result['lon'].to_numpy()[0], result['alt'].to_numpy()[0]]
    elif station in hydro_df['station_id'].to_numpy():
        result = hydro_df.loc[hydro_df['station_id'] == station]
        return [result['lat'].to_numpy()[0], result['lon'].to_numpy()[0]]
    else:
        raise ValueError("Please enter a valid station code.")


def show_maps() -> None:
    """
    In process...
    """
    meteo_df = pd.read_csv('https://api-datasette.konzept.space/existenz-api/smn_locations.csv?_size=max')
    hydro_df = pdf.read_csv('https://api-datasette.konzept.space/existenz-api/hydro_locations.csv?_size=max')
    crs = {'init': 'epsg:4326'}
    meteo_gdf = gpd.GeoDataFrame(meteo_df, crs=crs).set_geometry('geometry')
    hydro_gdf = gpd.GeoDataFrame(hydro_df, crs=crs).set_geometry('geometry')
    print(meteo_gdf)


def maps() -> None:
    """
    Function to help find the station code using a map.
    Prints two clickable links, one for the MeteoSwiss stations, one for the FOEN ones
    """
    print("https://api-datasette.konzept.space/existenz-api/smn_locations")
    print("https://api-datasette.konzept.space/existenz-api/hydro_locations")
