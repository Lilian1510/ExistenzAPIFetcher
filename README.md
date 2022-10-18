# :water: Existenz API Fetcher

This package is designed to automate the loading and use of sensor data from the MeteoSwiss Federal Office's
and the Federal Office for the Environment's (FOEN) networks of stations and is built on top of Christian Studer's ExistenzAPIs (https://api.existenz.ch/).

Data is obtained by querying a two-year, daily time series from the InfluxDB database and is returned as a `pandas` dataframe.
Functions to compute potential evapotranspiration using the `pyet` module have been implemented.
Take a look at the Jupyter Notebooks tutorials in the examples directory to learn how to use the various functionalities of the existenz_api_fetcher package.

## Requirements
```
pandas
influxdb-client
pyet
```

## :gear: Installation
To install the existenz_api_fetcher package from the PypI index:

`pip install existenz_api_fetcher`

To install in developer mode:

`pip install -e .`

