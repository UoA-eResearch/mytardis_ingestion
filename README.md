<!-- PROJECT LOGO -->
<br />
<p align="center">
  <h3 align="center">UoA MyTardis Ingestion Scripts</h3>

  <p align="center">
    Ingestion scripts for the University of Auckland's customised MyTardis. For each new instrument included into MyTardis, an instrument specific parser is needed. This parser will provide metadata to the ingestion scripts, by way of an Ingestion Factory instance.
	<!--
    <br />
    <a href="https://github.com/github_username/repo_name"><strong>Explore the docs »</strong></a>
    <br />-->
    <br />
    <a href="https://github.com/github_username/repo_name/issues">Report Bug</a>
    ·
    <a href="https://github.com/github_username/repo_name/issues">Request Feature</a>
  </p>
</p>

<!-- TABLE OF CONTENTS -->
## Table of Contents

* [About the Project](#about-the-project)
  * [Built With](#built-with)
* [Usage](#usage)
* [Roadmap](#roadmap)
* [Setup](#setup)
* [Contact](#contact)
  [Change Log](#change-log)

![main](https://github.com/UoA-eResearch/mytardis_ingestion/actions/workflows/mytardis_ingestion.yml/badge.svg)
[![codecov](https://codecov.io/gh/UoA-eResearch/mytardis_ingestion/branch/main/graph/badge.svg?token=GEI8FW6T1W)](https://codecov.io/gh/UoA-eResearch/mytardis_ingestion)
![interrogate](interrogate_badge.svg)
![lastcommit](https://img.shields.io/github/last-commit/UoA-eResearch/mytardis_ingestion/badge.svg)

## About the Project

[MyTardis](https://github.com/mytardis/mytardis) is an open-source scientific instrument data repository developed by Monash University. At the [University of Auckland](https://auckland.ac.nz) we have created a customised fork of the base repository to accommodate our specific researcher requirements. Part of this project involves the automated ingestion of research data into the repository and these scripts provide a standardised means of accessing the repository through the MyTardis API.

## Usage

Data ingestion into MyTardis via these ingestion scripts can be considered to have two parts, [metadata parsing](#metadata-parsing) followed by [data ingestion](#data-ingestion).

API keys, URLs for MyTardis and other information for ingestion purposes is held in **.env** files stored locally. A **pathlib** **Path** object is passed to the ingestion classes locating the config files, which are split into local and global config files, with the latter containing sensitive data beyond the scope of MyTardis (AD credentials, S3 credentials etc.) The **process_config()** function found in **config_helper.py** can be modified to account for additional API keys etc.

### Metadata Parsing

The data ingestion part of the ingestion process is required to be customised for different instruments and facility workflows. The goal of the metadata parser is to prepare the metadata into a standardised Python dictionary in preparation for ingestion.

#### Profiles

This process may include analysis of one or more run-file formats that are created by the instrument. Parsing is therefore customised on an instrument-by-instrument basis using Profiles. A Profile must be specified when ingesting metadata. Each extends the **_IProfile_** class, and contains a custom set of parsing functions run by an extractor. Extractors all extend the **_IMetadataExtractor_** class and use the **extract()** function to take a Path to instrument specific metadata files and parse them into a dictionary of python objects suitable for the **__IngestionFactory__**.

Current profiles are listed in [**profile_register.py**](src/profiles/profile_register.py).

#### The Ingestion Factory

The **_IngestionFactory_** class is an abstract base class for the specific profiles and the **process_object()** functions (where objects are the project/experiment/dataset and datafiles of the MyTardis hierarchy) should be defined in such a way as to extract and prepare the instrument metadata into an ingestible format.

### Data Ingestion

The data ingestion part of the ingestion process takes the prepared metadata dictionaries and calls the MyTardis API to create the objects in MyTardis. The **forge_object()** and **reforge_object()** functions allow the **_IngestionFactory_** to create the objects in MyTardis this way. Basic sanity checking is done on the input dictionaries to ensure that the minimum metadata required to create the appropriate object in MyTardis is present in the input dictionaries. We have also included functionality to mint and update RAiDs as identifiers for the different objects within MyTardis.

More information is available in the wiki for this repository.

### CLI

An ingestion pipeline can be run using the CLI command:
```Bash
ids ingest --profile profile_name metadata/source/data/path
```
This command loads the profile `profile_name`, based on that profile extracts any metadata in `metadata/source/data/path` then creates the corresponding metadata objects in MyTardis using API details specified by **.env** via the **_IngestionFactory_**.

For more information and options see

```Bash
ids --help
```



## Setup

[Setup instructions can be found here](setup/README.md)

## Documentation

For API documentation, Sphinx is used to autogenerate documents for this project. In order to generate the necessary documents for Sphinx, please follow the following steps:
1. Using terminal, change directory into the repo.
2. Activate the virtual environment by running the command "poetry shell" on the terminal.
3. On the terminal, run the command "cd docs".
4. Now run the command "source automake.sh", and this should automatically generate all the relevant docs.


## Contact

Contact Chris Seal (c.seal@auckland.ac.nz) for more information.
