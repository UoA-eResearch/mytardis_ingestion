"""JSON parser module.

Main objective of this module is to map the json files to
a format accepted by the mytardis_ingestion
"""


# ---Imports
import copy
import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.profiles import profile_consts as pc

# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class CustomBeneficiation(AbstractCustomBeneficiation):
    """A class to parse dataclass files into IngestibleDataclasses objects."""

    def __init__(
        self,
    ) -> None:
        pass

    def beneficiate(
        self,
        beneficiation_data: Any,
        ingestible_dclasses: IngestibleDataclasses,
    ) -> IngestibleDataclasses:
        """Parse dataclass files into IngestibleDataclasses objects.

        Args:
            beneficiation_data (Any): Data that contains information about the dataclasses to parse
            ingestible_dclasses (IngestibleDataclasses): object that conatins parsed dataclasses

        Returns:
            IngestibleDataclasses: object containing parsed dataclass objects.
        """
        proj_files: List[Path] = beneficiation_data[pc.PROJECT_NAME]
        expt_files: List[Path] = beneficiation_data[pc.EXPERIMENT_NAME]
        dset_files: List[Path] = beneficiation_data[pc.DATASET_NAME]
        dfile_files: List[Path] = beneficiation_data[pc.DATAFILE_NAME]

        ing_dclasses: IngestibleDataclasses = copy.deepcopy(ingestible_dclasses)
        logger.debug(f"proj_files = {str(proj_files)}")
        projects = self._parse_project_files(proj_files)
        ing_dclasses.add_projects(projects)

        logger.debug(f"expt_files = {str(expt_files)}")
        experiments = self._parse_experiment_files(expt_files)
        ing_dclasses.add_experiments(experiments)

        logger.debug(f"dset_files = {str(dset_files)}")
        datasets = self._parse_dataset_files(dset_files)
        ing_dclasses.add_datasets(datasets)

        logger.debug(f"dfile_files = {str(dfile_files)}")
        datafiles = self._parse_datafile_files(dfile_files)
        ing_dclasses.add_datafiles(datafiles)

        return ing_dclasses

    def _parse_project_files(
        self,
        dclass_files: List[Path],
    ) -> List[RawProject]:
        """Parse a given list of project files.

        Args:
            dclass_files (List[Path]): List of dataclass file paths.
            dclass (str): The name of the dataclass to be parsed.

        Returns:
            List[RawProject]: List of parsed RawProject objects.
        """
        return self._parse_json_files(dclass_files, RawProject)

    def _parse_experiment_files(
        self,
        dclass_files: List[Path],
    ) -> List[RawExperiment]:
        """Parse a given list of experiment files.

        Args:
            dclass_files (List[Path]): List of dataclass file paths.
            dclass (str): The name of the dataclass to be parsed.

        Returns:
            List[RawExperiment]: List of parsed RawExperiment objects.
        """
        return self._parse_json_files(dclass_files, RawExperiment)

    def _parse_dataset_files(
        self,
        dclass_files: List[Path],
    ) -> List[RawDataset]:
        """Parse a given list of dataset files.

        Args:
            dclass_files (List[Path]): List of dataclass file paths.
            dclass (str): The name of the dataclass to be parsed.

        Returns:
            List[RawDataset]: List of parsed RawDataset objects.
        """
        return self._parse_json_files(dclass_files, RawDataset)

    def _parse_datafile_files(
        self,
        dclass_files: List[Path],
    ) -> List[RawDatafile]:
        """Parse a given list of datafile files.

        Args:
            dclass_files (List[Path]): List of dataclass file paths.
            dclass (str): The name of the dataclass to be parsed.

        Returns:
            List[RawDatafile]: List of parsed RawDatafile objects.
        """
        return self._parse_json_files(dclass_files, RawDatafile)

    def _parse_json_files(
        self,
        dclass_files: List[Path],
        dclass_type: Any,
    ) -> List[Any]:
        """Casts the json object into the relevant raw dataclass

        Args:
            dclass_files (List[Path]): list of filepaths of a set of raw dataclasses
            dclass_type (Any): the type to parse into

        Returns:
            List[Any]: list of parsed raw dataclasses
        """
        raw_dclasses: List[Any] = []
        for fp in dclass_files:
            with fp.open("r") as f:
                json_obj = json.load(f)
                raw_dclass = dclass_type(**json_obj)
                raw_dclasses.append(raw_dclass)
        return raw_dclasses
