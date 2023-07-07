"""
YAML parser module. This module is used for parsing YAML files into
appropriate dataclasses.
"""

# Standard library imports
from copy import deepcopy
import logging
from pathlib import Path

# Third-party imports
import yaml
from yaml.loader import Loader
from yaml.nodes import MappingNode, Node

# User-defined imports
from src.profiles.idw.beneficiation_helpers.models import ( 
    RawProject,
   RawExperiment,
    RawDataset,
    RawDatafile,
)
from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.extraction_output_manager.ingestibles import IngestibleDataclasses

# Constants
logger = logging.getLogger(__name__)
prj_tag = "!Project"
expt_tag = "!Experiment"
dset_tag = "!Dataset"
dfile_tag = "!Datafile"
tags = [prj_tag, expt_tag, dset_tag, dfile_tag]

class CustomBeneficiation(AbstractCustomBeneficiation):
    """
    A class that provides methods to parse YAML files and construct objects.

    Attributes:
        None

    Methods:
    __init__():
        Initializes the CustomBeneficiation object and sets up the constructor functions for parsing YAML.

    _constructor_setup(loader, node) -> dict:
        A helper method that returns a dictionary containing the arguments of the constructor.

    _rawdatafile_constructor(loader, node) -> RawDatafile:
        A method that constructs a RawDatafile object using the constructor_setup helper method.

    _rawdataset_constructor(loader, node) -> RawDataset:
        A method that constructs a RawDataset object using the constructor_setup helper method.

    _rawexperiment_constructor(loader, node) -> RawExperiment:
        A method that constructs a RawExperiment object using the constructor_setup helper method.

    _rawproject_constructor(loader, node) -> RawProject:
        A method that constructs a RawProject object using the constructor_setup helper method.

    parse_yaml_file(fpath: str):
        A method that reads a YAML file, parses it and constructs objects using the constructor functions.
        Returns a list of objects constructed from the YAML file.
    """
    def __init__(
        self,
    ) -> None:
        """
        Initializes the CustomBeneficiation object and sets up the constructor functions for parsing YAML.

        Args:
            None

        Returns:
            None
        """
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

    def _constructor_setup(self, loader:Loader, node:MappingNode) -> dict:
        """
        A helper method that returns a dictionary containing the arguments of the constructor.

        Args:
            loader (Loader): A PyYAML loader object.
            node (Node): A PyYAML node object.

        Returns:
            dict: A dictionary containing the arguments of the constructor.
        """
        
        return dict(**loader.construct_mapping(node))

    def _rawdatafile_constructor(self, loader: Loader, node: MappingNode) -> RawDatafile:
        """
        A method that constructs a RawDatafile object using the constructor_setup helper method.

        Args:
            loader (Loader): A PyYAML loader object.
            node (Node): A PyYAML node object.

        Returns:
            RawDatafile: A RawDatafile object constructed from the YAML data.
        """
        return RawDatafile(**loader.construct_mapping(node))

    def _rawdataset_constructor(self, loader: Loader, node: MappingNode) -> RawDataset:
        """
        A method that constructs a RawDataset object using the constructor_setup helper method.

        Args:
            loader (Loader): A PyYAML loader object.
            node (Node): A PyYAML node object.

        Returns:
            RawDataset: A RawDataset object constructed from the YAML data.
        """
        return RawDataset(**loader.construct_mapping(node))

    def _rawexperiment_constructor(self, loader, node) -> RawExperiment:
        """
        A method that constructs a RawExperiment object using the constructor_setup helper method.

        Args:
            loader (Loader): A PyYAML loader object.
            node (Node): A PyYAML node object.

        Returns:
            RawExperiment: A RawExperiment object constructed from the YAML data.
        """
        return RawExperiment(**loader.construct_mapping(node))

    def _rawproject_constructor(self, loader: Loader, node: MappingNode) -> RawProject:
        """
        A method that constructs a RawProject object using the constructor_setup helper method.

        Args:
            loader (Loader): A PyYAML loader object.
            node (Node): A PyYAML node object.

        Returns:
            RawProject: A RawProject object constructed from the YAML data.
        """
        return RawProject(**loader.construct_mapping(node))

    #TODO Libby to convert all strings to pathlib.Path objects, and convert "loaded_data" into "ingestible_dataclasses" object.
    #The "ingestible_dataclasses" object is simply a list for each level in PEDD. This list contains the raw dataclasses.
    def parse_yaml_file(self, fpath: str) -> list:
        """
        Parse a YAML file at the specified path and return a list of loaded objects.

        Args:
            fpath (str): The path to the YAML file.
            ingestible_dataclasses (IngestibleDataclasses): _description_

        Returns:
            IngestibleDataclasses: _description_
        """
        logger.info("parsing {0}".format(fpath))
        with open(fpath) as f:
            data = yaml.safe_load_all(f)
            loaded_data = list(data)
            return loaded_data
        
    def beneficiate(self, data_loaded: list, ingestible_dataclasses: IngestibleDataclasses) -> IngestibleDataclasses:
        """
        Parse a YAML file at the specified path and return a list of loaded objects.

        Args:
            fpath (str): The path to the YAML file.

        Returns:
            List[Union[RawDatafile, RawDataset, RawExperiment, RawProject]]: A list of loaded objects.
        """
        
        ing_dclasses_out = self.beneficiate(data_loaded, ingestible_dataclasses = ingestible_dataclasses)

        return ing_dclasses_out