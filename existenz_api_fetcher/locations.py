import pandas as pd
import folium

meteo_df = pd.read_csv('https://api-datasette.konzept.space/existenz-api/smn_locations.csv?_size=max')
hydro_df = pd.read_csv('https://api-datasette.konzept.space/existenz-api/hydro_locations.csv?_size=max')


def geolocate(station: str) -> list:
    """
    Function to geolocate MeteoSwiss or FOEN hydrology stations.
    Args: station code (str)
    Returns: list with longitude, latitude, elevation (x, y, z) for MeteoSwiss and longitude, latitude (x, y) for FOEN
    """
    if station in meteo_df['code'].to_numpy():
        result = meteo_df[meteo_df['code'] == station]
        return [result['lon'].to_numpy()[0], result['lat'].to_numpy()[0], result['alt'].to_numpy()[0]]
    elif station in hydro_df['station_id'].to_numpy():
        result = hydro_df.loc[hydro_df['station_id'] == station]
        return [result['lon'].to_numpy()[0], result['lat'].to_numpy()[0]]
    else:
        raise ValueError("Please enter a valid station code.")


def show(meteo=True, hydro=True) -> folium.Map:
    """
    Displays the locations of the Meteo and/or Hydro stations on a Folium map.

    Parameters:
        meteo (bool): whether to show Meteo stations on the map
        hydro (bool): whether to show Hydro stations on the map

    Returns:
        A Folium map object displaying the station locations.
    """

    # Define the Switzerland coordinates and create a Folium map
    switzerland_coords = [46.8182, 8.2275]
    Map = folium.Map(location=switzerland_coords, zoom_start=8)

    # Show the Meteo stations on the map
    if meteo and not hydro:
        for idx, row in meteo_df.iterrows():
            folium.Marker(location=[row['lat'], row['lon']], popup=row.popup, icon=folium.Icon(color='green')).add_to(Map)

    # Show the Hydro stations on the map
    elif hydro and not meteo:
        for idx, row in hydro_df.iterrows():
            folium.Marker(location=[row['lat'], row['lon']], popup=row.popup, icon=folium.Icon(color='blue')).add_to(Map)

    # Show both the Meteo and Hydro stations on the map
    elif meteo and hydro:
        for name, location in zip(['Meteo', 'Hydro'], [meteo_df, hydro_df]):
            for idx, row in location.iterrows():
                marker_color = 'green' if name == 'Meteo' else 'blue'
                folium.Marker(location=[row['lat'], row['lon']], popup=row.popup, icon=folium.Icon(color=marker_color)).add_to(Map)

    # Raise an exception if neither Meteo nor Hydro stations are selected
    else:
        raise Exception('At least a station provider must be selected.')

    return Map


def links() -> None:
    """
    Function to help find the station code using a map.
    Prints two clickable links, one for the MeteoSwiss stations, one for the FOEN ones
    """
    print("https://api-datasette.konzept.space/existenz-api/smn_locations")
    print("https://api-datasette.konzept.space/existenz-api/hydro_locations")
