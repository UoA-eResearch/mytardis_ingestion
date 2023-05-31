"""ABI JSON parser module. 

Main objective of this module is to map the ABI json files to 
a format accepted by the mytardis_ingestion
"""


# ---Imports
import json
import logging

from pydantic import AnyUrl, ValidationError

from src.blueprints import RawDatafile, RawDataset, RawExperiment, RawProject
from src.profiles import profile_consts as pc
from src.utils.ingestibles import IngestibleDataclasses
from typing import Any, Optional

# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class Parser:
    """A class to parse dataclass files into IngestibleDataclasses objects."""

    def __init__(
        self,
    ) -> None:
        pass

    def parse(
        self,
        proj_files: list[str],
        expt_files: list[str],
        dset_files: list[str],
        dfile_files: list[str],
    ) -> IngestibleDataclasses:
        """Parse dataclass files into IngestibleDataclasses objects.

        Args:
            proj_files (list[str]): List of project file paths.
            expt_files (list[str]): List of experiment file paths.
            dset_files (list[str]): List of dataset file paths.
            dfile_files (list[str]): List of datafile file paths.

        Returns:
            IngestibleDataclasses: object containing parsed dataclass objects.
        """

        ingestible_dataclasses = IngestibleDataclasses()

        logger.debug("proj_files = {0}".format(str(proj_files)))
        projects = self._parse_dataclass_files(proj_files, pc.PROJECT_NAME)
        ingestible_dataclasses.add_projects(projects)

        logger.debug("expt_files = {0}".format(str(expt_files)))
        experiments = self._parse_dataclass_files(expt_files, pc.EXPERIMENT_NAME)
        ingestible_dataclasses.add_experiments(experiments)

        logger.debug("dset_files = {0}".format(str(dset_files)))
        datasets = self._parse_dataclass_files(dset_files, pc.DATASET_NAME)
        ingestible_dataclasses.add_datasets(datasets)

        logger.debug("dfile_files = {0}".format(str(dfile_files)))
        datafiles = self._parse_dataclass_files(dfile_files, pc.DATAFILE_NAME)
        ingestible_dataclasses.add_datafiles(datafiles)

        return ingestible_dataclasses

    def _parse_dataclass_files(
        self,
        dclass_files: list[str],
        dclass: str,
    ) -> list[Any]:
        """Parse a given list of dataclass files.

        Args:
            dclass_files (list[str]): List of dataclass file paths.
            dclass (str): The name of the dataclass to be parsed.

        Returns:
            list[Any]: List of parsed dataclass objects.
        """
        raw_dclasses: list[Any] = []
        dclass_type: Any
        if dclass == pc.PROJECT_NAME:
            dclass_type = RawProject
        elif dclass == pc.EXPERIMENT_NAME:
            dclass_type = RawExperiment
        elif dclass == pc.DATASET_NAME:
            dclass_type = RawDataset
        elif dclass == pc.DATAFILE_NAME:
            dclass_type = RawDatafile

        for fp in dclass_files:
            with open(fp, "r") as f:
                json_obj = json.load(f)
                raw_dclass = dclass_type(**json_obj)
                raw_dclasses.append(raw_dclass)

        return raw_dclasses
