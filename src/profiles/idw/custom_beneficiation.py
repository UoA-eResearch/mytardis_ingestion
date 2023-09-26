"""
YAML parser module. This module is used for parsing YAML files into
appropriate dataclasses.
"""

import logging

# Standard library imports
from copy import deepcopy
from pathlib import Path
from typing import Any, List, Union

# Third-party imports
import yaml
from yaml.loader import Loader
from yaml.nodes import MappingNode, Node

# User-defined imports
# from src.profiles.idw.beneficiation_helpers.models import IngestionMetadata
from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.blueprints import RawDatafile, RawDataset, RawExperiment, RawProject
from src.blueprints.common_models import GroupACL, UserACL
from src.blueprints.custom_data_types import Username
from src.extraction_output_manager.ingestibles import IngestibleDataclasses

# Constants
logger = logging.getLogger(__name__)
prj_tag = "!Project"
expt_tag = "!Experiment"
dset_tag = "!Dataset"
dfile_tag = "!Datafile"
groupacl_tag = "!GroupACL"
useracl_tag = "!UserACL"
username_tag = "!Username"
path_tag = "!Path"
tags = [
    prj_tag,
    expt_tag,
    dset_tag,
    dfile_tag,
    groupacl_tag,
    useracl_tag,
    username_tag,
    path_tag,
]


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
        yaml.add_constructor(
            prj_tag, self._rawproject_constructor
        )  # add object constructor
        yaml.constructor.SafeConstructor.add_constructor(
            prj_tag, self._rawproject_constructor
        )  # assign YAML tag to object constructor

        yaml.add_constructor(expt_tag, self._rawexperiment_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            expt_tag, self._rawexperiment_constructor
        )

        yaml.add_constructor(dset_tag, self._rawdataset_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            dset_tag, self._rawdataset_constructor
        )

        yaml.add_constructor(dfile_tag, self._rawdatafile_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            dfile_tag, self._rawdatafile_constructor
        )

        yaml.add_constructor(groupacl_tag, self._groupacl_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            groupacl_tag, self._groupacl_constructor
        )

        yaml.add_constructor(useracl_tag, self._useracl_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            useracl_tag, self._useracl_constructor
        )

        yaml.add_constructor(username_tag, self._username_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            username_tag, self._username_constructor
        )

        yaml.add_constructor(path_tag, self._path_constructor)
        yaml.constructor.SafeConstructor.add_constructor(
            path_tag, self._path_constructor
        )

    def _constructor_setup(self, loader: Loader, node: MappingNode) -> dict:
        """
        A helper method that returns a dictionary containing the arguments of the constructor.

        Args:
            loader (Loader): A PyYAML loader object.
            node (Node): A PyYAML node object.

        Returns:
            dict: A dictionary containing the arguments of the constructor.
        """

        return dict(**loader.construct_mapping(node))

    def _groupacl_constructor(self, loader: Loader, node: MappingNode) -> GroupACL:
        return GroupACL(**loader.construct_mapping(node))

    def _useracl_constructor(self, loader: Loader, node: MappingNode) -> UserACL:
        return UserACL(**loader.construct_mapping(node))

    def _username_constructor(self, loader: Loader, node: MappingNode) -> Username:
        return Username(node.value)

    def _path_constructor(self, loader: Loader, node: MappingNode) -> Path:
        return Path(node.value)

    def _rawdatafile_constructor(
        self, loader: Loader, node: MappingNode
    ) -> RawDatafile:
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

    def _rawexperiment_constructor(self, loader: Loader, node: MappingNode) -> RawExperiment:
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

    def beneficiate(
        self, fpath: Any, ingestible_dclasses: IngestibleDataclasses
    ) -> IngestibleDataclasses:
        """
        Parse a YAML file at the specified path and return a list of loaded objects.

        Args:
            fpath (str): The path to the YAML file.

        Returns:
            List[Union[RawDatafile, RawDataset, RawExperiment, RawProject]]: A list of loaded objects.
        """
        ingestible_dclasses = IngestibleDataclasses()
        logger.info("parsing {0}".format(fpath))
        with open(fpath) as f:
            data = yaml.safe_load_all(f)

            for obj in data:
                if isinstance(obj, RawProject):
                    ingestible_dclasses.add_project(obj)
                if isinstance(obj, RawExperiment):
                    ingestible_dclasses.add_experiment(obj)
                if isinstance(obj, RawDataset):
                    ingestible_dclasses.add_dataset(obj)
                if isinstance(obj, RawDatafile):
                    ingestible_dclasses.add_datafile(obj)
        return ingestible_dclasses
