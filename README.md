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

![master](https://github.com/UoA-eResearch/mytardis_ingestion/actions/workflows/mytardis_ingestion.yml/badge.svg)
[![codecov](https://codecov.io/gh/UoA-eResearch/mytardis_ingestion/branch/master/graph/badge.svg?token=GEI8FW6T1W)](https://codecov.io/gh/UoA-eResearch/mytardis_ingestion)
![interrogate](interrogate_badge.svg)
![lastcommit](https://img.shields.io/github/last-commit/UoA-eResearch/mytardis_ingestion/badge.svg)

## About the Project

[MyTardis](https://github.com/mytardis/mytardis) is an open-source scientific instrument data repository developed by Monash University. At the [University of Auckland](https://auckland.ac.nz) we have created a customised fork of the base repository to accommodate our specific researcher requirements. Part of this project involves the automated ingestion of research data into the repository and these scripts provide a standardised means of accessing the repository through the MyTardis API.

### Built With

* [Python](https://www.python.org/)

## Usage

Data ingestion into MyTardis via these ingestion scripts can be considered to have two parts, [metadata parsing](#metadata-parsing) followed by [data ingestion](#data-ingestion).

API keys, URLs for MyTardis and other information for ingestion purposes is held in **.env** files stored locally. A **pathlib** **Path** object is passed to the ingestion classes locating the config files, which are split into local and global config files, with the latter containing sensitive data beyond the scope of MyTardis (AD credentials, S3 credentials etc.) The **process_config()** function found in **config_helper.py** can be modified to account for additional API keys etc.

### Metadata Parsing

The data ingestion part of the ingestion process is required to be customised for different instruments and facility workflows. The goal of the metadata parser is to prepare the metadata into a standardised Python dictionary in preparation for ingestion.

This process may include analysis of one or more run-file formats that are created by the instrument, which is why the parser should be customised on an instrument-by-instrument basis. The **_IngestionFactory_** class is an abstract base class for the specific parsers and the **process_object()** functions (where objects are the project/experiment/dataset and datafiles of the MyTardis hierarchy)should be defined in such a way as to extract and prepare the instrument metadata into an ingestible format.

### Data Ingestion

The data ingestion part of the ingestion process takes the prepared metadata dictionaries and calls the MyTardis API to create the objects in MyTardis. The **forge_object()** and **reforge_object()** functions allow the **_IngestionFactory_** to create the objects in MyTardis this way. Basic sanity checking is done on the input dictionaries to ensure that the minimum metadata required to create the appropriate object in MyTardis is present in the input dictionaries. We have also included functionality to mint and update RAiDs as identifiers for the different objects within MyTardis.

**Minimum metadata requirements:**

 - Project Object.
   - **name**: The project name
   - **description**: A short project description
   - **raid**: A unique project identifier, RAiD for UoA project
   - **lead_researcher**: A username for the lead researcher in the project. This user will get admin access at all levels of the project and it's child objects. It should be noted that the UoA version of MyTardis authenticates against Active Directory and the API may need reworking for OAuth authentication.
   - **schema**: A schema name as defined within MyTardis for the Project level schema. This will include the metadata fields and short names associated with them.
   - Any additional keys in the project dictionary (with a couple of exceptions) will be added as metadata fields. If a match can be found in the schema, then this will be available for indexing for search. If not then the metadata will be added but may not be indexed.
 - Experiment Object.
   - **title**: The experiment name (*NB*: there are differences in the naming schemes between objects that may need tidying up - a legacy of the length of MyTardis development)
   - **raid**: A unique experiment identifier, RAiD for UoA experiments
   - **description**: A short description of the experiment
   - **project**: A project identifier (i.e. the **raid** field from the project object in question) for the parent project.
   - **schema**: A schema name as defined within MyTardis for the Experiment level schema. This will include the metadata fields and short names associated with them.
   - As with the project any additional keys will be added as metadata fields
 - Dataset Object.
   - **description**: The dataset name (see experiment **title** above)
   - **dataset_id**: A unique dataset identifier, RAiD for UoA datasets, could also be Dataset DOIs
   - **experiments**: A **list** of experiment identifiers associated with the experiment **raid**. We have assumed a one-to-many relationship between experiments and datasets, rather than the many-to-many relationship that is default in MyTardis. As such the scripts only get the first item in the list and would need refactoring to accommodate a many-to-many relationship.
   - [**instrument_id**](#instrument-metadata): A unique identifier to the instrument that the data was generated on. Currently there is no standard persistent identifier that has widespread community adoption (DOIs are the most likely candidate).
   - **schema**: A schema name as defined within MyTardis for the Dataset level schema. This will include the metadata fields and short names associated with them.
   - As with the project any additional keys will be added as metadata fields
 - Datafile Object. (*NB*: Given the limitations associated with transferring data through the *html* interface of MyTardis, we are streaming the data directly into our object store, using the **boto3** python library, and the **filehandler.py** script provides wrapper functions to do this. We then create a **replica** in MyTardis that points to the file location.
   - **filename**: The file name of the data file to be ingested
   - **md5sum**: The MD5 checksum of the original data file
   - **storage_box**: The MyTardis storage box defined for the facility
   - **local_path**: The full path to the local instance of the data file to be ingested
   _ **remote_path**: The relative path to the remote instance of the data file for the purposes of maintaining the local directory structure. This is in place to accommodate analysis packages that expect a specific directory structure.
   - **full_path**: The full path to the remote instance of the data file (normally constructed from the **remote_path** by the parser.
   - **schema**: A schema name as defined within MyTardis for the Datafile level schema. This will include the metadata fields and short names associated with them.
   - As with the project any additional keys will be added as metadata fields

### Instrument Metadata

Development of instrument persistent identifiers (PIDInst) has reached a point where we are comfortable beginning to use these in MyTardis. Instruments add to MyTardis from 2021 onward will have a PIDInst minted for them and this requires minimum metadata as described below.
 - **Landing Page**: A URL that the identifier resolves to.
 - **Name**: The instrument name
 - **Owner**: The institution(s) responsible for the management of the instrument
   - **Owner Name**: The full name of the owner
 - **Manufacturer**: The manufacturer or developer of the instrument
   - **Manufacturer Name**: The full name of the manufacturer

Recommended metadata fields include:
 - **Owner**:
   - **Owner Contact**: Contact email for the instrument owner
   - **Owner Identifier**: Persistent identifier (PID) for the instrument owner
   - **Owner Identifier Type**: The type of PID included.
 - **Manufacturer**:
   - **Manufacturer Identifier** PID for the manufacturer
   - **Manufacturer Identifier Type**: The type of PID included
 - **Model**: Name or model of the instrument as attributed by the manufacturer
   - **Model Name**: Full name of the Model
   - **Model Identifier**: PID for the model
   - **Model Identifier Type**: The type of PID included
 - **Description**: Technical description of the instrument and its capabilities
 - **Instrument Type**: Classification of the type of instrument
 - **Measured Variable**: What the instrument measures or observes
 - **Date**: Key dates include commissioning/decommissioning, calibration etc.
   -**Date Type**: What the date represents
 - **Related Identifier**: PIDs that are related to the instrument. For example a complex instrument might contain sensors that can be considered to be instruments in their own right. These could have PIDInst minted for them and they would list the other sensors in the instrument as related identifiers
   - **Related Identifier Type**: The type of PID included.
   - **Relation Type**: Description of the relationship
 - **Alternate Identifier**: Other Identifiers that the instrument has
   - **Alternate Identifier Type**: The type of identifier used as an alternate
### Roadmap

 - Migrate the API keys out of an **env** file and into a more secure information repository
 - Maintain parity with UoA MyTardis development to ensure that the ingestion scripts continue to function as expected
  - As part of ongoing MyTardis development an assessment of the cost/benefit of using GraphQL as an API in place of Tastypie will be made. Based on the outcome of that decision modifications to the ingestion scripts may be necessary.

## Setup

The setup is designed for an Ubuntu/Linux (recommended) or Manjaro/Linux setup with VS Code installed. For a generalised step-by-step setup for different OS environments, please refer to the Documentation channel on Teams, otherwise continue to see the following summarised steps (please note that the VS Code extensions are optional):

 ### Repo - Ubuntu/Manjaro

1.	Generate and add an SSH key for the device this repo is to be downloaded in on GitHub.
2.	Clone this repository via SSH.
3. On a terminal, cd into this repo's directory, and run the following command “source ./setup/<yourOS>_venv-setup.sh”, where <yourOS> is the operating system being used by the dev. A prompt will be displayed to enter the sudo password (if applicable). Please enter this to run the rest of the script. This will create and setup a virtual environment for the repo via the poetry method, and will also install pre-commit.
4.	Now the terminal is ready to run project scripts.

 ### Repo - Generalised Instructions

These instructions are written for non Ubuntu OS environments
1.	Install git, Python 3, and curl that can be run on a terminal. rsync are optionally required to run some tests.
2.	Create an SSH key for the device to link it to the dev’s GitHub account.
3.	Clone the mytardis_ingestion repo in the desired directory. This can be achieved on the terminal by first changing into the desired directory, then running the command “git clone git@github.com:UoA-eResearch/mytardis_ingestion.git”
4.	On the terminal, run the following command “curl -sSL https://install.python-poetry.org | python3 –“. This will install Poetry in the OS.
5.	Add the Poetry to the PATH environment variable.
6.	Run the following commands in this order (the explanation for each command is defined in the line above that command that start with #, and has been written to be friendly with terminal/bash):
  ```bash
	  # create a local virtual environment in the project directory
	  poetry config virtualenvs.in-project true
	  # in this case this command sets up the poetry file for installation
	  poetry lock
	  # installs all the relevant libraries for Python
	  poetry install
	  # installs git pre-commit
	  poetry run pre-commit install
	  # updates any relevant dependencies, used to avoid dependency issues
	  poetry run pre-commit autoupdate
  ```
7.	Run the command “poetry shell” to activate the virtual environment and the repo is now setup and ready to run the project.

 ### VS Code (OPTIONAL, and only recommended if this is a fresh install of the repo)

  1.  Open the “vscode_extensions.txt” file (found in the ./setup folder) and copy the commands for installing the extensions.
  2.	Using the terminal (or VS Code's terminal), paste the commands here and press the Enter key to install the relevant extensions required for VS Code.


## Documentation

For API documentation, Sphinx is used to autogenerate documents for this project. In order to generate the necessary documents for Sphinx, please follow the following steps:
1. Using terminal, change directory into the repo.
2. Activate the virtual environment by running the command "poetry shell" on the terminal.
3. On the terminal, run the command "cd docs".
4. Now run the command "source automake.sh", and this should automatically generate all the relevant docs.


## Contact

Contact Chris Seal (c.seal@auckland.ac.nz) for more information.
