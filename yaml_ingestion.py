"""NB: THIS WILL NOT WORK AS IT IS AND IS PROVIDED FOR INDICATIVE PURPOSES ONLY
Script to parse in the YAML files and create the objects in MyTardis.
The workflow for these scripts are as follows:
- Read in the YAML files and determine which contain Projects, Experiments, Datasets and Datafiles
- Process the files in descending order of Hierarchy in order to maximise the chance that parent
    objects exist
"""

import logging
import subprocess
from pathlib import Path

from src.helpers.config import ConfigFromEnv
from src.ingestion_factory.factory import IngestionFactory
from src.parsers.yaml_parser import YamlParser

logger = logging.getLogger(__name__)

yp = YamlParser()
yp.parse_yaml_file("test_renaming_exported_update.yaml")
