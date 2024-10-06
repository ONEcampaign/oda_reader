[![pypi](https://img.shields.io/pypi/v/oda_reader.svg)](https://pypi.org/project/oda_reader/)
[![python](https://img.shields.io/pypi/pyversions/oda_reader.svg)](https://pypi.org/project/oda_reader/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# ODA Reader - OECD DAC Data Importer

**ODA Reader** is a Python package that simplifies access to the **OECD DAC data**, leveraging
the **OECD data explorer API** and bulk downloads.

It allows for easy, programmatic access to OECD DAC data in python.

This documentation will walk you through how to set up and use ODA Reader, explaining its core features,
available modules, and providing examples of its use.

ODA Reader is a project created and maintained by The ONE Campaign.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Features](#features)
3. [Installation](#installation)
4. [Basic Usage](#basic-usage)
5. [DAC1](#1-downloading-dac1-data)
6. [DAC2a](#2-downloading-dac2a-data)

## Getting Started

**ODA Reader** provides a seamless way to access, download, and process data on Official Development Assistance (ODA)
and Other Official Flows (OOFs) directly from the OECD API.

## Features

- **Data Download Tools**: Easily download data from DAC1, DAC2a, the CRS and other datasets.
- **Query Builder**: Construct complex SDMX API queries easily using `QueryBuilder`.
- **Bulk download** microdata from the CRS, the Multisystem dataset, and other datasets.
- **Schema Translation**: Translate OECD data to `.stat` schema for easier integration.
- **Multi-version Support**: Access multiple versions of dataflows (CRS, DAC1, DAC2, etc.).

## Installation

The ODA Reader can be installed directly from the Python Package Index (PyPI) using pip.

To install from pip, simply run the following command:

```bash
pip install oda-reader
```

## Basic Usage

### 1. Downloading DAC1 Data

The `download_dac1()` function allows you to download DAC1 data. It accepts a few different
arguments:

- `start_year`: An integer like `2018`, specifying the starting year for the data.
  This parameter is optional - if not provided, the starting date for the dataset is used.
- `end_year`: An integer like `2022`, specifying the end year for the data.
  This parameter is optional - if not provided, the returned data goes up to the most recent year.
- `filters`: An optional dictionary containing additional filters to include in the API call.
  See the _Using Filters_ section for more details.
- `pre_process`: A boolean to specify if light cleaning of the data should be performed.
  If true, columns will be renamed to unique, machine readable names, and empty columns will be removed
- `dotstat_codes`: A boolean to specify if the API response should be translated to the dotstat schema.
  For this to work, `pre_process` must be true.
- `dataflow_version`: The specific schema / dataflow version to be used in the API call.
  This is an advanced parameter and should be used only if necessary to override the default.

This basic example will get all available data (all donors, all indicators, etc) from 2018 to 2022:

```python
from oda_reader import download_dac1

dac1_data = download_dac1(start_year=2018, end_year=2022)
```

You can also use filters to, for example, only get data for specific donors (here France and the United States):

```python
from oda_reader import download_dac1

dac1_data = download_dac1(
  start_year=2018, end_year=2022, filters={"donor": ["FRA","USA"]}
)
```

The filtering can get quite specific. For example, the following
query gets the total grant equivalents of loans from France in 2022 in national currency (current prices)

```python
from oda_reader import download_dac1

dac1_data = download_dac1(
  start_year=2022,
  end_year=2022,
  filters={
    "donor": "FRA",
    "measure": "11017",
    "flow_type": "1160",
    "unit_measure": "XDC",
    "price_base": "V",
  },
)
```

By default, ODA Reader performs basic preprocessing of the returned data, and it converts the response to the OECD.Stat schema. These options can be turned off to get the data exactly as returned by the API.

```python
from oda_reader import download_dac1

dac1_data = download_dac1(pre_process=False, dotstat_codes=False)
```

Pre-processing converts column names to distinct machine-readable names, and it sets the right data types for further analysis with Pandas. The data can also be pre-processed without translating to the OECD.Stat schema.

```python
from oda_reader import download_dac1

dac1_data = download_dac1(pre_process=True, dotstat_codes=False)
```

### 2. Downloading DAC2a Data

The `download_dac2a()` function allows you to download DAC2a data. It accepts a few different
arguments:

- `start_year`: An integer like `2018`, specifying the starting year for the data.
  This parameter is optional - if not provided, the starting date for the dataset is used.
- `end_year`: An integer like `2022`, specifying the end year for the data.
  This parameter is optional - if not provided, the returned data goes up to the most recent year.
- `filters`: An optional dictionary containing additional filters to include in the API call.
  See the _Using Filters_ section for more details.
- `pre_process`: A boolean to specify if light cleaning of the data should be performed.
  If true, columns will be renamed to unique, machine readable names, and empty columns will be removed
- `dotstat_codes`: A boolean to specify if the API response should be translated to the dotstat schema.
  For this to work, `pre_process` must be true.
- `dataflow_version`: The specific schema / dataflow version to be used in the API call.
  This is an advanced parameter and should be used only if necessary to override the default.

This basic example will get all available data (all donors, all recipients, indicators, etc) from 2018 to 2022:

```python
from oda_reader import download_dac2a

dac2_data = download_dac2a(start_year=2018, end_year=2022)
```

You can also use filters to, for example, only get data for specific recipients:

```python
from oda_reader import download_dac2a

dac2_data = download_dac2a(
  start_year=2018, end_year=2022, filters={"recipient": ["TGO", "NGA"]}
)
```

The filtering can get quite specific. For example, the following
query gets the imputed multilateral aid from the UK to Guatemala and China in 2022 in constant prices.

```python
from oda_reader import download_dac2a

dac2_data = download_dac2a(
  start_year=2022,
  end_year=2022,
  filters={
    "donor": "GBR",
    "recipient": ["GTM","CHN"],
    "measure": "106",
    "price_base": "Q",
  },
)
```

By default, ODA Reader performs basic preprocessing of the returned data, and it converts the response to the OECD.Stat schema. These options can be turned off to get the data exactly as returned by the API.

```python
from oda_reader import download_dac2a

dac2_data = download_dac2a(pre_process=False, dotstat_codes=False)
```

Pre-processing converts column names to distinct machine-readable names, and it sets the right data types for further analysis with Pandas. The data can also be pre-processed without translating to the OECD.Stat schema.

```python
from oda_reader import download_dac2a

dac2_data = download_dac2a(pre_process=True, dotstat_codes=False)
```
