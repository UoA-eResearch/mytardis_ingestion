"""
YAML parser module. This module is used for parsing YAML files into
appropriate dataclasses.
"""

# Standard library imports
from copy import deepcopy
from pathlib import Path
from typing import Union
import logging
import os
from dataclasses import dataclass, field

# Third-party imports
import yaml

# User-defined imports
'''
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
'''
from src.helpers.config import (
    ConfigFromEnv,
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
)
from src.smelters.smelter import Smelter
from src.parsers.models import (  ### create new data model to match YAML file
    IngestionMetadata,
    RawProject,
   RawExperiment,
    RawDataset,
    RawDatafile,
)

# Constants
logger = logging.getLogger(__name__)
prj_tag = "!Project"
expt_tag = "!Experiment"
dset_tag = "!Dataset"
dfile_tag = "!Datafile"
tags = [prj_tag, expt_tag, dset_tag, dfile_tag]

class YamlParser:
    """YamlParser class to
    Attributes:
    """
    def __init__(
        self,
    ) -> None:
        #yaml.add_constructor(prj_tag, self._rawproject_constructor) #add object constructor
        yaml.constructor.SafeConstructor.add_constructor(
            prj_tag, self._rawproject_constructor
        ) #assign YAML tag to object constructor

        #yaml.add_constructor(expt_tag, self._rawexperiment_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            expt_tag, self._rawexperiment_constructor
        )

        #yaml.add_constructor(dset_tag, self._rawdataset_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            dset_tag, self._rawdataset_constructor
        )

        #yaml.add_constructor(dfile_tag, self._rawdatafile_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            dfile_tag, self._rawdatafile_constructor
        )

    def _constructor_setup(self, loader, node) -> dict:
        return dict(**loader.construct_mapping(node))

    def _rawdatafile_constructor(self, loader, node) -> RawDatafile:
        return RawDatafile(**loader.construct_mapping(node))

    def _rawdataset_constructor(self, loader, node) -> RawDataset:
        return RawDataset(**loader.construct_mapping(node))

    def _rawexperiment_constructor(self, loader, node) -> RawExperiment:
        return RawExperiment(**loader.construct_mapping(node))

    def _rawproject_constructor(self, loader, node) -> RawProject:
        return RawProject(**loader.construct_mapping(node))

    def parse_yaml_file(self, fpath: str):
        logger.info("parsing {0}".format(fpath))
        with open(fpath) as f:
            
            data = yaml.safe_load_all(f)
            loaded_data = list(data)
            return loaded_data