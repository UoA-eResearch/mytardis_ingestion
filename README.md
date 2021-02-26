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
* [Contact](#contact)
  [Change Log](#change-log)

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

### Change Log

commit cdaa832c9ecfe171d3d43d68c86a3181787822b8
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 18 09:30:04 2020 +1300

    Update naming to reflect changes in base models

commit 1de2a6e314e8167458180ee78854fdc2cc1e74cd
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 4 10:18:47 2020 +1300

    Fix broken file comparison that was causing datafiles to try to repost

commit f2e691185cb1e82ed8540c0fd227177d83b99efd
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Sat Oct 17 10:22:53 2020 +1300

    Revert Filehandler

commit b3bd86e30d1c66225f4cf5716bfe76f6717fe67e
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 18:05:10 2020 +1300

    Try S3Transfer object to get around AccessDenied error

commit d01c86318b7c60766a60ef42745d56fc5eadfbb4
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 17:44:40 2020 +1300

    Trying again with multipart uploads

commit 48f981f9f9cf7fbb144590721d87838a7b9552c1
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 17:28:09 2020 +1300

    Try without specific multipart upload threshold

commit 500ef2bc63633d425627209249ec5d7fa7c938a2
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 16:53:39 2020 +1300

    premature cast to string

commit eeed74cc87ccb11e96a0b7282593989645935a82
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 16:50:59 2020 +1300

    Fixed typo

commit 64ff64b63995c17614f88faa93ae7a62a62d5387
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 16:49:00 2020 +1300

    Update upload to use TransferConfig setup

commit 39d33a58ee9dbf0486a970ccc0577a1859aa665a
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 15:41:40 2020 +1300

    Fix up upload function to account for None type remote_root

commit cafdd21ee55a939be5e30784c124805250c5246f
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 15:39:32 2020 +1300

    Check for None in remote_root during initialisation

commit 0c2661f6453e29e5d08706a07ef4a5304803b0ea
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 16 15:36:19 2020 +1300

    Fixed Filehandler to account for None remote

commit ae9a8e4a644e8747aa1bc8adb7a7ae198c9306e3
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Oct 15 11:59:05 2020 +1300

    Fixed typo

commit 9676eaff7b6d8155ee815ecf849f44e11db2bea5
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Oct 15 11:57:16 2020 +1300

    Add list_files functionality to S3FileHandler class

commit 357d2e49cd5fdff28ff17fbaa9d7a1b385e0730f
Merge: abff4d9 33b206e
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Oct 15 11:09:37 2020 +1300

    Merge branch 'UoA-develop'

commit 33b206e1f623d2970beb3ea798be618271310bc8
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Oct 15 11:09:21 2020 +1300

    Added __init__.py to facilitate loading S3FileHandler class

commit 04df61d972dc3eb5831ee178f2ca90ac904c31c8
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Oct 15 09:59:47 2020 +1300

    Tidy up and add additional functionality to S3FileHandler class

commit e5ac09e1eae50447771f6b7a63eb1859a718e47c
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Oct 14 16:07:32 2020 +1300

    Tidy up and add s3_tools

commit c636942e7bd65349f9c1618ec711bac3b6392869
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Oct 14 15:18:02 2020 +1300

    Tidy up

commit cf207ef5b12033e4085941584194132ff121c296
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Oct 14 11:30:07 2020 +1300

    Fixed Typos and error raising

commit abff4d9169d6df65aa2329f6d4fa8cf766180fd0
Merge: fdd2f9d 753cc18
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Oct 13 09:57:05 2020 +1300

    Merge branch 'UoA-Dev-Chris'

commit fba28c358125d8dbb0a8a781796ba589c1e1e77b
Merge: 7656eef 753cc18
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Oct 13 09:56:53 2020 +1300

    Merge branch 'UoA-Dev-Chris' into UoA-develop

commit 753cc185d5cddf4ed762819e8ba7257df110d12c
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Oct 13 09:56:22 2020 +1300

    Updates and add Readme.md

commit 4f76c3c44aa26cfe9a1e5f00e69d65a1ede08309
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Oct 6 13:00:41 2020 +1300

    Add PUT to experiment and project

commit 8c3f978c9588bff0775b41ac045a410b2d963396
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Aug 21 11:52:09 2020 +1200

    Remove broken code chunk and added S3 requirements to requriements.txt

commit ea73cb56fddf7d9442635bb3403720a250fbf433
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Aug 21 08:34:33 2020 +1200

    Add requirements for ingestion and bugfixing

commit 72bb702ef416456f81ae6236891d2911d7f59f5c
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Aug 20 12:28:57 2020 +1200

    Update .gitignore

commit 769fdb216f3a8039f96e0df28e749eda21d98220
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Aug 20 12:22:25 2020 +1200

    Added function to return all owned RAiDs

commit bd3128c6e07be69a6a16b20725d607818cbbf3dd
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Aug 20 12:01:53 2020 +1200

    Refactored to use the process_config function

commit 726e1151b5cc746e088bb53db600125432cf2f61
Merge: a891066 fdd2f9d
Author: Chris <c.seal@auckland.ac.nz>
Date:   Thu Aug 13 15:35:59 2020 +1200

    Merge branch 'master' into UoA-Dev-Chris

commit fdd2f9ddea25c73f5ef8b5d230e9f226dca2ff05
Author: Chris <c.seal@auckland.ac.nz>
Date:   Thu Aug 13 15:35:43 2020 +1200

    Add lists3 test

commit ce7fb8f65b7ed00d9e95b0ee4fd3529a469b2f69
Merge: a5d8efd a891066
Author: Chris <c.seal@auckland.ac.nz>
Date:   Tue Aug 11 14:39:28 2020 +1200

    Merge remote-tracking branch 'origin/UoA-Dev-Chris'

commit a891066b83c24d729a8dca36ca50ca897b38e2d6
Author: Chris <c.seal@auckland.ac.nz>
Date:   Tue Aug 11 14:38:44 2020 +1200

    Ingestion scripts for new models working as at 11 Aug 2020

commit 9fb3e60360d4417041d12e06921dd6395d7f767c
Author: Chris <c.seal@auckland.ac.nz>
Date:   Tue Aug 11 10:18:20 2020 +1200

    Fix

commit 6be7750342f1c1f0ae958bd26dcacc78d20715ea
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Aug 10 14:29:54 2020 +1200

    Datafile ingestion debugging

commit bcea948c780af1933683b816e09ff146f888cec0
Author: Chris <c.seal@auckland.ac.nz>
Date:   Thu Aug 6 16:12:15 2020 +1200

    Debugging continues

commit 9d494723015bea91019cbe9865fa7b530f443ed0
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Aug 3 17:50:02 2020 +1200

    Update .gitignore

commit 4ae4b16a3eaf98a3330d53cb3e1877411e1829ef
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Aug 3 17:48:00 2020 +1200

    Project and experiment ingest working as desired

commit 1c89dc781187bc6b0be84d6a26ba0e22259e213a
Author: Chris <c.seal@auckland.ac.nz>
Date:   Wed Jul 22 14:45:46 2020 +1200

    Abstract factory class created

commit ae682c5a08cd627c1ea2ed80a171908fdb651a32
Author: Chris <c.seal@auckland.ac.nz>
Date:   Tue Jul 21 16:30:17 2020 +1200

    Removed unnecessary calls to global config files

commit 237f4cf2563d29f960592c43837815894386b357
Author: Chris <c.seal@auckland.ac.nz>
Date:   Tue Jul 21 16:22:24 2020 +1200

    Project/experiment/dataset forges

commit 14a233d40bc73bc0ad69883d157ab21f8a22332e
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Jul 13 16:50:45 2020 +1200

    Project Forge completed

commit 9ea2852d2f387b70e7cbbd64aefdaa5273681057
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Jul 6 16:12:21 2020 +1200

    Foundry is not needed - smelting should be pushed to the forges

commit 77436c457e3856f3a275d50bf6fc9d0865e0e9ef
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Jul 6 16:11:38 2020 +1200

    Schema validation implemented

commit 7a2cbaaee8897a32742d8595cc468c69be320636
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Jul 6 13:31:15 2020 +1200

    Updated Datafile minion and Overseer

commit b9cb5f9a6e8153790e4b02ed21e339663bb857f5
Author: Chris <c.seal@auckland.ac.nz>
Date:   Fri Jul 3 11:38:04 2020 +1200

    Added hierarchy stuff to overseer

commit 3b932d3764e2426aa6a43dede0ccfc135702d50b
Author: Chris <c.seal@auckland.ac.nz>
Date:   Thu Jul 2 16:00:28 2020 +1200

    TODOs

commit 292fbb03b6454e2ae1c030f10116ac434bbf9a4d
Author: Chris <c.seal@auckland.ac.nz>
Date:   Thu Jul 2 15:29:41 2020 +1200

    Update Overseer and Minions

commit aaf55994f261ee5c467e3118c94c2e0829628e2a
Author: Chris <c.seal@auckland.ac.nz>
Date:   Mon Jun 29 10:40:11 2020 +1200

    refactor

commit 7656eefcb4e534c48e027fae31f114e5e99a347a
Merge: d2e424b 8744f7e
Author: Mike Laverick <mikelaverick@btinternet.com>
Date:   Wed Jun 10 10:43:25 2020 +1200

    Merge pull request #1 from UoA-eResearch/UoA-Dev-Chris
    
    Merge UoA-Dev-Chris into UoA-develop

commit 540b3b8ebef1590f33b6ec7103ff475ca6467ca4
Author: Chris <c.seal@auckland.ac.nz>
Date:   Fri May 29 16:08:06 2020 +1200

    Split out helper functions for clarity

commit 91bf69cf62e5c063c794c09ff28c91f13b8fb96e
Author: Chris <c.seal@auckland.ac.nz>
Date:   Fri May 29 09:57:17 2020 +1200

    Refactor structure to make ingestor function more clear

commit 8744f7e67d45d408e3fc57d68a57255ef592c69f
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri May 15 17:30:21 2020 +1200

    Refactor ingestor for new models

commit fad206c2c4810dbfd4063922bcc59b714ab0f2e8
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Mar 10 16:41:41 2020 +1300

    Upload other files from directory

commit ee805a53886b508d4aee42797010cea58ff15e5b
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Mar 10 16:18:49 2020 +1300

    Bug fixes

commit 40ba9506202cbab89b649f6d5761a0f469d96e83
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 9 18:08:03 2020 +1300

    Import Path lib

commit 0083a82d4b26a4a8c3025901091b9bcaaff23708
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 9 17:21:27 2020 +1300

    Fixed missing meta in __imaging_process

commit 52059bf8a23f5182577cae8f1116c493386b58b5
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 9 16:29:44 2020 +1300

    Debugging

commit 4f1e17c50c864dde2dd30ce792f7e0a77557d692
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 9 16:27:40 2020 +1300

    WIP ensuring upload_dir function gives expected outcome

commit 29131207fd6883c377d263e14fb0f67e85b6288d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 9 16:10:34 2020 +1300

    Updated filehandler to push all files into s3 including non-zip files

commit 684372f8c493edfb78c2543200699cac3621b498
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 9 15:56:37 2020 +1300

    Exposed do_ldap_search function as public

commit 24395e8e428b4a38db2630d92ac700e9742080bc
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 9 15:19:34 2020 +1300

    MALDI Imaging updates

commit a5d8efd97608063fd2edf58d6a07a79438c8eb4a
Merge: 8b40607 d2e424b
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Mar 4 17:20:12 2020 +1300

    Merge branch 'UoA-develop'

commit d2e424b12cb8748f822ad33894410533e402d9d2
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Mar 4 17:18:51 2020 +1300

    Bug squishing complete - solarix parser working apart from MALDI Imaging

commit 0cad54705a94529568e63d92b239e72984d38a18
Merge: 1183c0d 6b0efa1
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Mar 4 09:20:44 2020 +1300

    resolved merge conflict

commit 1183c0d2d7a1191f4bf07196cf56e98c8ba9ce1e
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Mar 4 09:16:17 2020 +1300

    Debugging

commit a57b7ec5cc385cad423212730688b9745f7f0fd0
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Mar 4 09:14:33 2020 +1300

    Debugging

commit 6b0efa17504f1dccef36d2960e6146577ec2f316
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 2 16:25:55 2020 +1300

    Remove old TODO comment

commit b9173ea1163a8f8001c71bbe6f9b8ce4ea353880
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 2 14:51:09 2020 +1300

    removed explicit super

commit 5d6538012dc8d2fd0d5b5082d3397b2fdf56a7f9
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 2 14:44:45 2020 +1300

    unified checksum digests and moved to JSON

commit 0351f975152b8e85f428f5e3b0988404178bcfdb
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 2 14:10:26 2020 +1300

    Copy to staging and uploading whole directories in filehandler

commit b2d4f490478245b3498393cb265408554f56b63e
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Mar 2 11:55:37 2020 +1300

    Solarix parser refactored

commit bfa0e77282f7b5a430883b36d0c840be9a293fb2
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Feb 24 17:46:22 2020 +1300

    End of the day commit

commit f19eeb892e00b17ca33cfd844c9c9814609a3445
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Feb 21 15:55:31 2020 +1300

    Fixed casting to dictionaries from env files

commit 15391ae1c1ca4c88519358a7a11d22a7ac3d3f83
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Feb 21 15:28:13 2020 +1300

    LDAP and ProjectDB updates

commit 3b2d559526c9dbbed96d09afcbcc3cd715ea0c86
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Feb 20 17:03:54 2020 +1300

    Current pos

commit 23fe455d322e1539d4ed8069a256c75ad3fc8c77
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Feb 20 16:44:41 2020 +1300

    Add RAiD update to ingestor - requires a RAiD in the parsing process

commit 4223b05b13f78b0479e025fdeb01d0f1e7f69186
Author: mike-ivs <mike.laverick@auckland.ac.nz>
Date:   Wed Feb 19 22:35:33 2020 +0000

    Removed logger info from unittest output

commit c6416f0fc32854881bd09ff98a9f234205a2e384
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Feb 18 14:55:38 2020 +1300

    ProjectDBFactory and RaIDFactory written

commit f0a51f4cc9843611948df2802887b09670f1abb2
Merge: 71fee89 4274c91
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Feb 18 11:36:43 2020 +1300

    Merge remote-tracking branch 'origin/UoA-develop' into UoA-develop

commit 71fee894fc27a5cdbb8b51e9e3dd7735704da8a9
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Feb 18 11:36:36 2020 +1300

    RAiD get working

commit 11a0c925a07fe59ae20ff055db285dd8fc725fe1
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Feb 17 17:59:22 2020 +1300

    Begining RaID stuff

commit 4274c910ebb1456e8d8811264fa1627ba267d05c
Merge: 02aff4e 012dad5
Author: mike-ivs <mike.laverick@auckland.ac.nz>
Date:   Mon Feb 17 04:25:48 2020 +0000

    Merge branch 'UoA-develop' of https://github.com/UoA-eResearch/mytardis_ingestion into UoA-develop

commit 02aff4eb04beb5080fac39374d559d9de4a24606
Author: mike-ivs <mike.laverick@auckland.ac.nz>
Date:   Mon Feb 17 04:25:29 2020 +0000

    Renamed test_helper to test_modules. Added tests for mail handler

commit 012dad5bfdfcaf35deff9a1e293130240636c764
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Feb 17 17:02:19 2020 +1300

    Update filehandler to use decouple library

commit 761144dde61342f847b8c4e4637a7dc304589817
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Feb 17 13:25:48 2020 +1300

    MyTardisUploader class refactored ready for testing

commit 9e3106b486c676fb75db1df88b1f33795e0aaf1d
Merge: a5e0413 f10ab54
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Feb 17 11:31:05 2020 +1300

    Merge remote-tracking branch 'origin/UoA-develop' into UoA-develop

commit a5e0413ddf56bfd111cdebef44161101d9646eb8
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Feb 17 11:30:56 2020 +1300

    Refactored function to spilt datafile dictionaries

commit f10ab541098ed5198691b269fb155f6c5a2b90fc
Author: mike-ivs <mike.laverick@auckland.ac.nz>
Date:   Fri Feb 14 03:53:17 2020 +0000

    Added imports and logging to helper/ldap.py. Tested functionality

commit b8b6228c1030786f67d114bbabaf28f6d53b54c6
Merge: d06f9f1 96d39c8
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Feb 14 16:16:09 2020 +1300

    Merge remote-tracking branch 'origin/UoA-develop' into UoA-develop

commit d06f9f1215d2097330e792a05cb48bb95dafcaa7
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Feb 14 16:15:56 2020 +1300

    Added detail in README.md

commit 96d39c88716d83766e4943ec59e2918bdc5f0a88
Merge: 75bb0a1 9ae88b7
Author: mike-ivs <mike.laverick@auckland.ac.nz>
Date:   Fri Feb 14 03:14:34 2020 +0000

    Merge branch 'UoA-develop' of https://github.com/UoA-eResearch/mytardis_ingestion into UoA-develop

commit 75bb0a1709ae3a8ff70cffc1b2657cfa0d3ce5d7
Author: mike-ivs <mike.laverick@auckland.ac.nz>
Date:   Fri Feb 14 03:12:34 2020 +0000

    added unittests for helper functions. Fixed calculate_etags function

commit 9ae88b77056652dd953d1a4918ce615eac970ce7
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Feb 14 15:16:25 2020 +1300

    Brief documentation about ingestion dictionaries

commit ffd52b1b04dd070d281f8ff888d6056e83baa53d
Merge: 1fed074 19597fa
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Feb 14 09:34:53 2020 +1300

    Merge remote-tracking branch 'origin/UoA-develop' into UoA-develop

commit 1fed07472458bba1ba8e6fbb75d91a18ce54c7ff
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Feb 14 09:34:05 2020 +1300

    Push LDAP functions to separate module

commit 19597fa1fc26d67539a530b168cc90e9fb08518e
Author: mike-ivs <mikelaverick@btinternet.com>
Date:   Thu Feb 13 04:17:07 2020 +0000

    additional comments

commit b0ca948681896e677a6ab91c74aae42ce5415b63
Author: Ubuntu <ubuntu@playground.novalocal>
Date:   Thu Feb 13 04:03:46 2020 +0000

    testunits for JSON I/O and lowercase functions

commit d8323b6d43692e344d3e8a052853c247e1e82318
Merge: ebe77e1 ad3273b
Author: Ubuntu <ubuntu@playground.novalocal>
Date:   Wed Feb 12 22:33:44 2020 +0000

    Merge branch 'UoA-develop' of https://github.com/UoA-eResearch/mytardis_ingestion into UoA-develop

commit ebe77e15524ee7b6a6b003b1f32d6ebe61131f23
Author: Ubuntu <ubuntu@playground.novalocal>
Date:   Wed Feb 12 22:33:06 2020 +0000

    Initialised test script for helper functions

commit ad3273b147fee92e5f2b4b0395e206661fe6659f
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Feb 13 11:28:26 2020 +1300

    Refactored s3 file handler class

commit 38ceb2bf18f9eb167d614830f2ef501098608299
Author: Ubuntu <ubuntu@playground.novalocal>
Date:   Wed Feb 12 21:44:36 2020 +0000

    commented out old helper function imports in csv_parser

commit 8b2ff90a9b28ce166c7d3a22ff8fe277d19d6b22
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Feb 11 17:07:57 2020 +1300

    Refactoring file handler begins

commit 03f0d0ceca58ef858d79e45d88fa1db60120ef2a
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Feb 11 15:22:52 2020 +1300

    Version 1 of new harvester script

commit 369f13d34c8e78c38ae0369c623c79b94404b009
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Nov 12 16:50:12 2019 +1300

    Modifications to filehandler and removed print statements

commit 0c51507a82406b5acc0df2212e02afc8ba488088
Merge: 4b9df3b a709544
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Nov 11 12:56:12 2019 +1300

    Merge branch 'UoA-develop' of https://github.com/UoA-eResearch/mytardis_ingestion into UoA-develop

commit 4b9df3b7653924da4ecfcde18ed49ebb697b0c55
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Nov 11 12:55:37 2019 +1300

    Everything except s3 uploading tested and working

commit a7095446040ff08b6f4705f7e3433809c3d8ebf9
Merge: 4d1e47a f1a8207
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Nov 7 17:28:40 2019 +1300

    Merge remote-tracking branch 'origin/UoA-develop' into UoA-develop

commit f1a82078631e3131f61151230c62fe116a5294ee
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Nov 7 17:24:14 2019 +1300

    Bug fixes for experiment and datasets from solarix

commit 4d1e47ace1fb8d2a83701c3cd0721cd62096cefa
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 17:30:22 2019 +1300

    Filehandler update

commit 76f726432b74f18b9b5ddcf4822e48f4b880f8f2
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 16:10:45 2019 +1300

    more typos to fix

commit a0ed8bc63edee92254f1107195e70600cf35b509
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 15:49:34 2019 +1300

    Pop file from list

commit ca5aaa671d0e9500f42198d32ae841f39d73ba4b
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 15:45:25 2019 +1300

    Debugging

commit 0d414f5fa6ccb4c48314df1973e83f8bae6dcf02
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 13:07:41 2019 +1300

    Filehandler testing

commit 50ef61322785402e03eff7f7cca62f50df3c6330
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 11:57:01 2019 +1300

    Changes for testing purposes

commit 0dd1e0cc79be63d1732fe184fe2052c38d550094
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 11:18:09 2019 +1300

    Staging server bug fixes

commit fe29e03a79a4061b20041d984783412afcaf05ab
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Nov 6 10:40:18 2019 +1300

    Update ldap to ldap3

commit c4888969bdf8261f240a9fed77a92a1abb3e5c45
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Nov 5 16:14:29 2019 +1300

    Bug fixes on the staging server

commit 7b33021bf6c03191eb93bf5796ae15d10b1aa150
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Nov 5 15:03:05 2019 +1300

    Try this

commit e16f936868a9a0866eb8020d3795b44d4083a85b
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Nov 5 15:00:27 2019 +1300

    Debuging

commit d6547662fc9497e92707252eecf3c729247b40e8
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Nov 5 14:15:01 2019 +1300

    Typo

commit ccac4561fdddbc69eb6354ad3ff289563540efe3
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Nov 5 13:51:50 2019 +1300

    Each component test ready

commit 4b0f487daab93871b2d1fed0e29b965024da1a0d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Nov 5 09:13:30 2019 +1300

    Putting it all together

commit 520c26e8434f1b8d9f1e3cc9f6b72aab16c5398c
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Nov 4 15:43:47 2019 +1300

    Commit current progress

commit c14fbaa05e7b40e7c58f9a2688087a196aa65348
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Oct 18 16:41:01 2019 +1300

    Moving to harvester-subclass model

commit 33e5987074c13e778d7f2286697a3b5c8cd81371
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Oct 16 11:36:33 2019 +1300

    create user when it doesn't exist

commit a45363d38ccd728a1d1c3f9156ab5ad06977d3f3
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 30 16:49:27 2019 +1300

    Mostly finished create_user function

commit d21c23d4499f3b6b4a8390b93ee12c2fc4f2db54
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 30 11:15:18 2019 +1300

    Fix naming

commit 290cc06af691406ad522258aed2b584e5fb574bd
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 30 11:10:15 2019 +1300

    Fixed local vs staging

commit 294d02be85d3d923134b9f34ad345709086b2345
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 30 11:06:38 2019 +1300

    Added move to staging for s3filehandler and built archive builder for solarix

commit 540241943a32cd8e23020e8c5640cb3958d005d6
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Sep 26 16:41:35 2019 +1200

    Various solarix fixes

commit de02c518afc099b9d1a381f7600fd44d863c20a1
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Sep 26 14:08:05 2019 +1200

    Solarix parser builds experiment and dataset dictionaries correctly

commit 6be02ed0b7784eb95af9d91c96e262fa4f17dc59
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Sep 25 16:15:15 2019 +1200

    Added project stuff

commit 9c79f2eb19620ceffe80f026c99bbe59685f7cfe
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Sep 25 10:42:07 2019 +1200

    Added user/owner split

commit 9f18daadb4c8fdc9f918314a6ff0ffda7a1d844b
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Sep 24 16:28:54 2019 +1200

    Added date string

commit 8a0321b97109f9096c4d8481d7f7e6756e461044
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Sep 24 14:50:43 2019 +1200

    Lookup project details for subdirectories

commit 6639ba79f9e6c8db65b4455ce9fcd2c1909caaf2
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Sep 24 13:08:02 2019 +1200

    LDAP lookup on email from project DB

commit cadb43e2fda4a577e977eed8ee301f8a68b6d9ce
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 23 16:38:14 2019 +1200

    added Path objects using pathlib in order to identify  projects in tree

commit 5f193074556505bee84a4009bb0e024ce89ae96d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Sep 19 15:36:23 2019 +1200

    Change default ACL deets

commit 553b292a0faf4e35e51de2526ee79e59e57ad8a9
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Sep 17 16:00:15 2019 +1200

    Minor updates

commit cb90e40e4bab15194b7d1478685642bff52d8a48
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 9 12:12:29 2019 +1200

    Mailserver added

commit 88b81d1b782a04b6e60f1e18202723caa558aa1c
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Sep 4 17:18:35 2019 +1200

    Preliminary upload of the solarix parser

commit 6538f13b563abccdb65fcd75b0e4c431449d3cdf
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Sep 4 10:20:49 2019 +1200

    Refactored s3 Uploader

commit 333e424909d699de1a69913d00fede5e3d7a428d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Sep 3 17:03:15 2019 +1200

    Beginning refactoring

commit 16b18cb0034bda7b9fbb3f932a5dbe60e14043d5
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Sep 3 17:02:48 2019 +1200

    Beginning s3 refactoring

commit e6bd0816429abfd8e1d569d13779bb2f4b9ee13f
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Sep 3 10:02:25 2019 +1200

    Datafile dictionaries returned from parser

commit 58fdd027e5a6bfc2946c4773ae60efd3ce3f195d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 2 18:27:42 2019 +1200

    Bug fixes

commit 51442925175ef6db42a57210a845dd0f26916727
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 2 15:28:00 2019 +1200

    dataset dictionaries building as expected, multiple sets of metadata handled by CSV parser

commit 7ae6a64c6b2ca57b40ead194779b119a599e3302
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 2 13:57:54 2019 +1200

    Refactoring in progress

commit 1d4063b5845204933b8aba6c20d85d337b092ef3
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 2 09:30:48 2019 +1200

    Renaming old files

commit fbf25f446d7dc563f574716f3f1898e71cbce4e3
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Mon Sep 2 09:08:27 2019 +1200

    fix library paths

commit 2e084a4a28b1cc881a18e8b4dc534ebff19df3de
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Aug 30 16:23:02 2019 +1200

    Refactoring of CSV parser

commit c33add2ab7665db56078b27845864a1b87595a1d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Aug 30 13:01:52 2019 +1200

    Abstract classes for Parser and FileHandler added

commit 6e2e589b982738559570de9a5a3cddf26a84f44c
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Aug 30 11:52:24 2019 +1200

    Refactor mytardis uploader to be remove messy file handling since this is being handled by the file uploader class

commit 28e532bf3186a34ea53a2d804151b5d1c52780fc
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Fri Aug 30 11:06:35 2019 +1200

    Move parsers directory

commit 78a0bde8fc797834e56b0795e756a7f6d7823ce3
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Aug 29 13:56:09 2019 +1200

    Finished ingestor create dataset functions

commit aa97e37fbcf4b5bb5764d88f896b8656ef82110d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Thu Aug 29 11:24:04 2019 +1200

    Finished ingestor create experiment functions

commit 3197ff972b4a465ae1916aaf4882df697fca83b0
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Aug 28 17:17:22 2019 +1200

    refactor and simplify

commit 8b406078d243a242078f9321eb41236126f0bd30
Merge: 195106d 16fd455
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Aug 28 09:56:04 2019 +1200

    Merge remote-tracking branch 'origin/UoA-develop' into UoA-develop

commit 195106ddf31f89e18ac7f2a2177a696b27fec111
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Aug 28 09:55:16 2019 +1200

    More abstracting of classes

commit a7f181cf3688cb61fdb24550ade8fbb110c4ed29
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 16:06:07 2019 +1200

    More logging stuff - centralising to a mytardis log

commit ab350ecf67ab77e12f6ea2942b1365899ff1c8f9
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 12:26:10 2019 +1200

    Refactor genomics parser

commit 5a066b05607f73fdf7f6e45360b80540f5d4b377
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 14:59:20 2019 +1200

    Refactoring to push functions to helper and to deal with duplicate datasets

commit 5f4d5cfd3cd1bfaeaeca82464d1d45e3486a6b7a
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 12:26:10 2019 +1200

    Refactor genoics parser

commit 9888572c0f2413829bd8b56c79e926d6e9739725
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Wed Aug 28 09:07:45 2019 +1200

    Abstracting a Harvester class

commit 16fd4552131434e1459566e01617dedac63c581f
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 16:06:07 2019 +1200

    More logging stuff - centralising to a mytardis log

commit 482982fd4076a6d09281af762480a121c16c988d
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 14:59:20 2019 +1200

    Refactoring to push functions to helper and to deal with duplicate datasets

commit 2a1dbfcb6eb1930a53dcafa25d917d2ad8644dff
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 12:26:10 2019 +1200

    Refactor genoics parser

commit fc92496bac8fbe515793eedc87b53657b600387a
Author: Chris Seal <c.seal@auckland.ac.nz>
Date:   Tue Aug 27 12:19:09 2019 +1200

    Tidying up and clearing cruft history

<!-- End of change log -->

## Contact

Contact Chris Seal (c.seal@auckland.ac.nz) for more information.
