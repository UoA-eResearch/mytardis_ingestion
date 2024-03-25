# pylint: disable-all
# This needs a lot of refactoring so disable checks
# noqa
# nosec

"""Part of the Extraction Plant where generated metadata files are parsed
into raw dataclasses.
"""


# ---Imports
import logging
from typing import Any, Dict

from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.extraction.manifest import IngestionManifest

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Beneficiation:
    """
    This class provides a means of beneficiating project, experiment, dataset and datafile files.
    It takes a list of files in the specified format and parses them into a dictionary of objects.
    """

    def __init__(
        self,
        beneficiation: AbstractCustomBeneficiation,
    ) -> None:
        self.beneficiation = beneficiation

    def beneficiate(self, beneficiation_data: Dict[str, Any]) -> IngestionManifest:
        """Parse metadata files of a given file type into raw dataclasses

        Args:
            beneficiation_data (Dict[str, Any]): Data that contains information about the dataclasses to parse
            ingestible_dataclasses (IngestionManifest): A class that contains the raw datafiles, datasets, experiments, and projects.

        Returns:
            IngestionManifest: A class that contains the raw datafiles, datasets, experiments, and projects.
        """
        logger.info("beneficiating")

        ing_dclasses_out = self.beneficiation.beneficiate(
            beneficiation_data=beneficiation_data
        )

        logger.info(f"ingestible projects = {ing_dclasses_out.get_projects()}")
        logger.info(f"ingestible experiments = {ing_dclasses_out.get_experiments()}")
        logger.info(f"ingestible datasets = {ing_dclasses_out.get_datasets()}")
        logger.info(f"ingestible datafiles = {ing_dclasses_out.get_datafiles()}")

        return ing_dclasses_out
