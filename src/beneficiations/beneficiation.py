"""Part of the Extraction Plant where generated metadata files are parsed
into raw dataclasses.
"""


# ---Imports
import copy
import logging
from typing import Any, Dict, Union

from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.config.singleton import Singleton
from src.extraction_output_manager.ingestibles import IngestibleDataclasses

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Beneficiation(metaclass=Singleton):
    """
    This class provides a means of beneficiating project, experiment, dataset and datafile files.
    It takes a list of files in the specified format and parses them into a dictionary of objects.
    """

    def __init__(
        self,
        beneficiation: AbstractCustomBeneficiation,
    ) -> None:
        self.beneficiation = beneficiation

    def beneficiate(
        self,
        beneficiation_data: Dict[str, Any],
        ingestible_dataclasses: IngestibleDataclasses,
    ) -> IngestibleDataclasses:
        """Parse metadata files of a given file type into raw dataclasses

        Args:
            beneficiation_data (Dict[str, Any]): Data that contains information about the dataclasses to parse
            ingestible_dataclasses (IngestibleDataclasses): A class that contains the raw datafiles, datasets, experiments, and projects.

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects.
        """
        logger.info("beneficiating")

        ing_dclasses_out = self.beneficiation.beneficiate(
            beneficiation_data=beneficiation_data,
            ingestible_dclasses=ingestible_dataclasses,
        )

        logger.info(f"ingestible projects = {ing_dclasses_out.projects}")
        logger.info(f"ingestible experiments = {ing_dclasses_out.experiments}")
        logger.info(f"ingestible datasets = {ing_dclasses_out.datasets}")
        logger.info(f"ingestible datafiles = {ing_dclasses_out.datafiles}")

        return ing_dclasses_out
