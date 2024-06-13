# pylint: disable-all
# type: ignore
# This needs a lot of refactoring so disable checks

"""
YAML parser module. This module is used for parsing YAML files into
appropriate dataclasses.
"""

import logging

# Standard library imports
from pathlib import Path

# Third-party imports
import yaml

# User-defined imports
from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.blueprints import RawDatafile, RawDataset, RawExperiment, RawProject
from src.extraction.manifest import IngestionManifest
from src.miners.utils import datafile_metadata_helpers
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

    def beneficiate(self, yaml_path: Path) -> IngestionManifest:
        """
        Parse a YAML file at the specified path and return a list of loaded objects.

        Args:
            fpath (str): The path to the YAML file.

        Returns:
            List[Union[RawDatafile, RawDataset, RawExperiment, RawProject]]: A list of loaded objects.
        """
        if not yaml_path.is_file() or yaml_path.suffix != ".yaml":
            raise ValueError(f"{yaml_path} is not a valid YAML file.")

        ingestible_dclasses = IngestionManifest(source_data_root=yaml_path.parent)
        logger.info("parsing {0}".format(yaml_path))

        with open(yaml_path) as f:
            data = yaml.safe_load_all(f)
            for obj in data:
                if isinstance(obj, RawProject):
                    ingestible_dclasses.add_project(obj)
                if isinstance(obj, RawExperiment):
                    ingestible_dclasses.add_experiment(obj)
                if isinstance(obj, RawDataset):
                    ingestible_dclasses.add_dataset(obj)
                if isinstance(obj, RawDatafile):
                    df_path = yaml_path.parent / obj.filepath
                    obj.md5sum = datafile_metadata_helpers.calculate_md5sum(df_path)
                    ingestible_dclasses.add_datafile(obj)
        return ingestible_dclasses
