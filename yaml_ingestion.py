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
from src.smelters.smelter import Smelter
from src.overseers.overseer import Overseer

logger = logging.getLogger(__name__)

yp = YamlParser()
data_load = yp.parse_yaml_file("test_renaming_exported_update.yaml")
print(data_load)
'''

'''
#config = ConfigFromEnv()
#print(config)
### test 
#config: ConfigFromEnv = None

#smelter = Smelter(
#        overseer: Overseer,
#        general: GeneralConfig,
#        default_schema: SchemaConfig,
#        storage: StorageConfig,
#)
#factory = IngestionFactory()
'''
yp = YamlParser()
data_load = yp.parse_yaml_file("test_renaming_exported_update.yaml")
projects = data_load.projects
experiments = data_load.experiments
datasets = data_load.datasets
datafiles = data_load.datafiles
print(projects)
'''

#for project_file in projects:
    #factory.ingest_projects(project_file)
