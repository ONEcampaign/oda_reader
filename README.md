[![pypi](https://img.shields.io/pypi/v/oda_reader.svg)](https://pypi.org/project/oda_reader/)
[![python](https://img.shields.io/pypi/pyversions/oda_reader.svg)](https://pypi.org/project/oda_reader/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# ODA Reader
The OECD DAC Data Importer

**ODA Reader** is a Python package that simplifies access to the **OECD DAC data**, leveraging
the **OECD data explorer API** and bulk downloads.

It allows for easy, programmatic access to OECD DAC data in python. It is designed for policy
analysts, data analysts, researchers and students who need easy and programmatic access to 
OECD DAC data.

This documentation will walk you through how to set up and use ODA Reader.

ODA Reader is a project created and maintained by The ONE Campaign.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Features](#features)
3. [Installation](#installation)
4. [DAC1](#downloading-dac1-data)
5. [DAC2a](#downloading-dac2a-data)
6. [CRS](#downloading-crs-data)
7. [Multisystem](#downloading-multisystem-data)
8. [Using filters](#using-filters)
9. [Contribute](#contributing-to-oda-reader)

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

### Downloading DAC1 Data

The `download_dac1()` function allows you to download DAC1 data from the data-explorer API.
It accepts a few different arguments:

- `start_year`: An integer like `2018`, specifying the starting year for the data.
  This parameter is optional - if not provided, the starting date for the dataset is used.
- `end_year`: An integer like `2022`, specifying the end year for the data.
  This parameter is optional - if not provided, the returned data goes up to the most recent year.
- `filters`: An optional dictionary containing additional filters to include in the API call.
  See the [Using filters](#using-filters) section for more details.
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

### Downloading DAC2a Data

The `download_dac2a()` function allows you to download DAC2a data from the data-explorer API.
It accepts a few different arguments:

- `start_year`: An integer like `2018`, specifying the starting year for the data.
  This parameter is optional - if not provided, the starting date for the dataset is used.
- `end_year`: An integer like `2022`, specifying the end year for the data.
  This parameter is optional - if not provided, the returned data goes up to the most recent year.
- `filters`: An optional dictionary containing additional filters to include in the API call.
  See the [Using filters](#using-filters) section for more details.
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

### Downloading CRS Data

The `download_crs()` function allows you to download CRS data from the data-explorer API.
It accepts a few different arguments:

- `start_year`: An integer like `2018`, specifying the starting year for the data.
  This parameter is optional - if not provided, the starting date for the dataset is used.
- `end_year`: An integer like `2022`, specifying the end year for the data.
  This parameter is optional - if not provided, the returned data goes up to the most recent year.
- `filters`: An optional dictionary containing additional filters to include in the API call.
  See the [Using filters](#using-filters) section for more details.
- `pre_process`: A boolean to specify if light cleaning of the data should be performed.
  If true, columns will be renamed to unique, machine readable names, and empty columns will be removed
- `dotstat_codes`: A boolean to specify if the API response should be translated to the dotstat schema.For this to work, `pre_process` must be true.
- `as_grant_equivalent`: A boolean to specify whether the 'flows' or 'grant equivalent' version of the CRS should be returned.
- `dataflow_version`: The specific schema / dataflow version to be used in the API call.
  This is an advanced parameter and should be used only if necessary to override the default.

**Note** the `download_crs` function defaults to getting 'microdata'. That means project-level data. This is different from the approach taken by the data-explorer online, which shows semi-aggregated data. In order to view semi-aggregates, you can add `microdata: False` to the filters.

This API is quite slow, and the data can quickly get quite large. It is recommended to use filters to limit the data returned, or to use the bulk download feature (`bulk_download_crs`) to avoid repeated, slow calls to the API.

This basic example will get all available data (all donors, all recipients, indicators, etc) from 2018 to 2022. It will return 'flows' data.

```python
from oda_reader import download_crs

crs_data = download_crs(start_year=2018, end_year=2022)
```

The same example as grant equivalents would be:

```python
from oda_reader import download_crs

crs_data = download_crs(start_year=2018, end_year=2022, as_grant_equivalent=True)
```

You can also use filters to, for example, only get data for specific donors and recipients:

```python
from oda_reader import download_crs

crs_data = download_crs(
  start_year=2018, end_year=2022, filters={"donor": "DEU", "recipient": ["TGO", "NGA"]}
)
```

The filtering can get quite specific. For example, the following
query gets disbursements for ODA grants from Germany to Nigeria for
primary education, provided through multilateral organisations, in 
constant prices:

```python
from oda_reader import download_crs

crs_data = download_crs(
  start_year=2022,
  end_year=2022,
  filters={
    "donor": "DEU",
    "recipient": "NGA",
    "sector": "11220",
    "measure":"11",
    "channel": "40000",
    "price_base": "Q",
  },
)
```

The data-explorer API can also return semi-aggregates, built from the CRS microdata. 
That is the data that is shown online through the data-explorer. 

You can get that view of the data using the ODA Reader package. However, the filters must
be used to avoid double counting.

For example, to get all ODA from the United States to Liberia in 2019. In this case
`channel` and `modality` are set to `_T` (which stands for total). Alternatively, it can
be set to specific channels or modalities to get semi-aggregates for specific channels
or modalities.

```python
from oda_reader import download_crs

crs_data = download_crs(
  start_year=2019,
  end_year=2019,
  filters={
    "donor": "USA",
    "recipient": "LBR",
    "sector": "1000",
    "measure":"100",
    "channel": "_T",
    "modality": "_T",
    "flow_type": "C",
    "price_base": "V",
    "microdata": False,
  },
)
```

By default, ODA Reader performs basic preprocessing of the returned data, and it converts the response to the OECD.Stat schema. These options can be turned off to get the data exactly as returned by the API.

```python
from oda_reader import download_crs

crs_data = download_crs(pre_process=False, dotstat_codes=False)
```

Pre-processing converts column names to distinct machine-readable names, and it sets the right data types for further analysis with Pandas. The data can also be pre-processed without translating to the OECD.Stat schema.

```python
from oda_reader import download_crs

crs_data = download_crs(pre_process=True, dotstat_codes=False)
```

#### Bulk downloading CRS data

In many situations, downloading the full CRS may be the most efficient way to conduct analysis. For example, when requesting a lot of data, or when all the project information is needed.

For those cases, ODA Reader provides tools for getting the bulk download files provided by the OECD.
The entire CRS is provided as a parquet file (just over 1GB in size). They also provide a 'reduced'
version which does not include certain columns in order to result in a smaller file.

The `bulk_download_crs()` function allows you to download the full CRS data (as a parquet file)
It accepts a few different arguments:

- `save_to_path`: A string or `Path` object specifying a folder where the parquet file should be
saved. If not provided, `bulk_download_crs` will return a Pandas DataFrame.
- `reduced_version`: A boolean which defaults to `False`. If `True` smaller file (removing certain
columns) is downloaded and saved/returned instead.

**Note** that the files provided by the OECD follow the .Stat schema.

To save the full parquet file to `example-folder`:

```python
from oda_reader import bulk_download_crs

bulk_download_crs(save_to_path="./example-folder/")
```

To keep the full file in memory as a Pandas DataFrame:

```python
from oda_reader import bulk_download_crs

full_crs = bulk_download_crs()
```

To download the smaller file to `example-folder`:

```python
from oda_reader import bulk_download_crs

bulk_download_crs(save_to_path="./example-folder/", reduced_version=True)
```

To keep the smaller file in memory as a Pandas DataFrame:
```python
from oda_reader import bulk_download_crs

full_crs = bulk_download_crs(reduced_version=True)
```

The `download_crs_file()` function allows you to download the CRS data for a specific year
(as a parquet file). It accepts a few different arguments:

- `year`: An integer specifying the year needed (e.g 2019).
- `save_to_path`: A string or `Path` object specifying a folder where the parquet file should be
  saved. If not provided, `download_crs_file` will return a Pandas DataFrame.

**Note** that the files provided by the OECD follow the .Stat schema.

To save the full parquet file to `example-folder`:

```python
from oda_reader import download_crs_file

download_crs_file(year=2022, save_to_path="./example-folder/")
```

For older years, the years are grouped in a single file. For example:
- 2004-05
- 2002-03
- 2000-01
- 1995-99
- 1973-94

In those cases, the year string can be passed as `year` to download the file.
For example, for 1995-1999:

```python
from oda_reader import download_crs_file

download_crs_file(year="1995-99", save_to_path="./example-folder/")
```

To keep the file in memory as a Pandas DataFrame, for 2017 for example:

```python
from oda_reader import download_crs_file

crs_data = download_crs_file(year=2017)
```


### Downloading Multisystem Data

The `download_multisystem()` function allows you to download _Members total use of the
multilateral system (Multisystem)_ data from the data-explorer API.

It accepts a few different arguments:

- `start_year`: An integer like `2018`, specifying the starting year for the data.
  This parameter is optional - if not provided, the starting date for the dataset is used.
- `end_year`: An integer like `2022`, specifying the end year for the data.
  This parameter is optional - if not provided, the returned data goes up to the most recent year.
- `filters`: An optional dictionary containing additional filters to include in the API call.
  See the [Using filters](#using-filters) section for more details.
- `pre_process`: A boolean to specify if light cleaning of the data should be performed.
  If true, columns will be renamed to unique, machine readable names, and empty columns will be removed
- `dotstat_codes`: A boolean to specify if the API response should be translated to the dotstat schema. For this to work, `pre_process` must be true.
- `dataflow_version`: The specific schema / dataflow version to be used in the API call.
  This is an advanced parameter and should be used only if necessary to override the default.

This API is quite slow, and the data can quickly get quite large. It is recommended to use filters to limit the data returned, or to use the bulk download feature (`bulk_download_multisystem`) to avoid repeated, slow calls to the API.

This basic example will get all available data (all donors, all recipients, indicators, etc) from 2018 to 2022.

```python
from oda_reader import download_multisystem

multisystem = download_multisystem(start_year=2018, end_year=2022)
```

You can also use filters to, for example, only get data for specific donors and recipients:

```python
from oda_reader import download_multisystem

multisystem = download_multisystem(
  start_year=2018, end_year=2022, filters={"donor": "ITA", "recipient": ["TGO", "NGA"]}
)
```

The filtering can get quite specific. For example, the following
query gets core multilateral contributions from Canada to the World Bank International
Development Association (IDA), as disbursements in current prices.

```python
from oda_reader import download_multisystem

multisystem = download_multisystem(
  start_year=2015,
  end_year=2015,
  filters={
    "donor": "CAN",
    "channel": "44002",
    "flow_type": "D",
    "price_base": "Q"
  },
)
```

By default, ODA Reader performs basic preprocessing of the returned data, and it converts the response to the OECD.Stat schema. These options can be turned off to get the data exactly as returned by the API.

```python
from oda_reader import download_multisystem

multisystem = download_multisystem(pre_process=False, dotstat_codes=False)
```

Pre-processing converts column names to distinct machine-readable names, and it sets the right data types for further analysis with Pandas. The data can also be pre-processed without translating to the OECD.Stat schema.

```python
from oda_reader import download_multisystem

multisystem = download_multisystem(pre_process=True, dotstat_codes=False)
```

#### Bulk downloading Multisystem data

In many situations, downloading the full Multisystem data may be the most efficient way to conduct analysis. For example, when requesting a lot of data, or when all the project information is needed.

For those cases, ODA Reader provides tools for getting the bulk download files provided by the OECD.
The entire Multisystem dataset is provided as a parquet file.

The `bulk_download_multisystem()` function allows you to download the full CRS data (as a parquet file).

It accepts a single argument:

- `save_to_path`: A string or `Path` object specifying a folder where the parquet file should be
  saved. If not provided, `bulk_download_multisystem` will return a Pandas DataFrame.

**Note** that the files provided by the OECD follow the .Stat schema.

To save the full parquet file to `example-folder`:

```python
from oda_reader import bulk_download_multisystem

bulk_download_multisystem(save_to_path="./example-folder/")
```

To keep the full file in memory as a Pandas DataFrame:

```python
from oda_reader import bulk_download_multisystem

full_multisystem = bulk_download_multisystem()
```

## Using filters
When using ODA Reader, you can apply filters to refine the data you retrieve from the API. This applies to all tools except for the bulk download functions.

Filters allow you to specify subsets of data, making it easy to focus on the information that is most relevant to your needs.

Filters are specified as a dictionary, with keys representing the filter categories (such as donor, recipient, sector, etc.) and values representing the criteria to match, provided as single values (like a year, or a code), or lists of values (like multiple donors or multiple sectors). 

You can use the `get_available_filters()` function to see the available filter parameters that
can be used for a specific dataset. Note that (for now) all filter values must be provided using
the data-explorer schema.

For example, to get the available filters for DAC1:

```python
from oda_reader import get_available_filters

dac1_filters = get_available_filters(source="dac1")
```

By default, the dictionary is also printed. To only return the object without printing,
you can set `quiet` as `True`.

```python
from oda_reader import get_available_filters

dac1_filters = get_available_filters(source="dac1", quiet=True)
```

The same applies to other sources:

```python
from oda_reader import get_available_filters

dac2a_filters = get_available_filters(source="dac2a")
crs_filters = get_available_filters(source="crs")
multisystem_filters = get_available_filters(source="multisystem")
```


## Contributing to ODA Reader

Thank you for your interest in contributing to ODA Reader. We welcome contributions from everyone to help improve this project.

Note that this project is not associated with, nor maintained by, the OECD.

### Submitting Ideas and Reporting Issues

If you have an idea for a new feature, additional functionality, or if you have encountered a bug, please feel free to submit an issue to initiate a discussion. This helps ensure alignment and prevents duplicated efforts.

### Contributing Code

To contribute code, you can fork the repository, implement your changes, and then open a pull request (PR). Please ensure that you submit an issue beforehand to discuss your proposed changes.

Your contributions are invaluable in making ODA Reader better for everyone.
