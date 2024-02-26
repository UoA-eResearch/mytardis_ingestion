# pylint: disable-all
# type: ignore
# This needs a lot of refactoring so disable checks

"""
YAML parser module. This module is used for parsing YAML files into
appropriate dataclasses.
"""

import logging
import os

# Standard library imports
from copy import deepcopy
from pathlib import Path
from typing import Any, List, Union

# Third-party imports
import yaml

# User-defined imports
from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.blueprints import RawDatafile, RawDataset, RawExperiment, RawProject
from src.extraction.ingestibles import IngestibleDataclasses
from src.helpers.dataclass import get_object_name
from src.miners.utils import datafile_metadata_helpers
from src.mytardis_client.enumerators import DataStatus as EnumDataStatus
from src.profiles.idw.yaml_helper import YamlParser

# Constants
logger = logging.getLogger(__name__)

# Initialise the representers and constructors required for
# loading YAML elements.
YamlParser = YamlParser()


class CustomBeneficiation(AbstractCustomBeneficiation):
    """
    CustomBeneficiation class for parsing
    YAML files into appropriate dataclasses.
    """

    def beneficiate(
        self, fpath: Path, ingestible_dclasses: IngestibleDataclasses
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
                    print(obj)
                    ingestible_dclasses.add_project(obj)
                if isinstance(obj, RawExperiment):
                    ingestible_dclasses.add_experiment(obj)
                if isinstance(obj, RawDataset):
                    ingestible_dclasses.add_dataset(obj)
                if isinstance(obj, RawDatafile):
                    df_path = Path(fpath).parent.joinpath(obj.directory)
                    df_dir = df_path / obj.filename
                    obj.md5sum = datafile_metadata_helpers.calculate_md5sum(df_dir)
                    ingestible_dclasses.add_datafile(obj)
        return ingestible_dclasses
