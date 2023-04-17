"""NB: THIS WILL NOT WORK AS IT IS AND IS PROVIDED FOR INDICATIVE PURPOSES ONLY
Script to parse in the YAML files and create the objects in MyTardis.
The workflow for these scripts are as follows:
- Read in the YAML files and determine which contain Projects, Experiments, Datasets and Datafiles
- Process the files in descending order of Hierarchy in order to maximise the chance that parent
    objects exist (TODO: write code to do this)
- In the processing phase, check to see if an object with the identifier exists (TODO write code for
    when no identifier app is being used)
- If the object exists check to see that the metadata is the same and report if not or overwrite
    using a PUT request if specifically requested.(TODO: write code to do this)
- Process the YAML into object and parameter dictionaries which will require additional calls to
    ensure that related objects exist (TODO: write code to do this)
- Using the Forge create the object within MyTardis (TODO: write code to do this)
- Move files from their current location to their final location (TODO: write code to do this)
- Collate successes and failures and prepare a report to be emailed (TODO: write code to do this)
"""

import logging
import subprocess
from pathlib import Path

from src.helpers.config import ConfigFromEnv
from src.ingestion_factory.factory import IngestionFactory
from src.parsers.yaml_parser import YamlParser
from src.smelters.smelter import Smelter
from src.overseers.overseer import Overseer

logger = logging.getLogger(__name__)

yp = YamlParser()

data_load = yp.parse_yaml_file("test_renaming_exported_update.yaml")
print(data_load[0].name)

