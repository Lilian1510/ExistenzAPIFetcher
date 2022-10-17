# Existenz API Fetcher

This package is designed to automate the loading and use of data from the MeteoSwiss Federal Office 
and the Federal Office for the Environment (FOEN).
The software is built on top of Christian Studer's ExistenzAPIs (https://api.existenz.ch/).

## Requirements
Data is obtained by querying a time series from the InfluxDB database and is returned in the form of a `pandas` dataframe.
Potential evapotranspiration can be computed using the `pyet` package.
Jupyter Notebooks tutorials on how to use the various functionalities of the package are shown in the examples directory.

## Installation
To install the existenz_api_fetcher package from the Pypi index:

>>> pip install existenz_api_fetcher

To install in developer mode, use the following syntax:

>>> pip install -e .

