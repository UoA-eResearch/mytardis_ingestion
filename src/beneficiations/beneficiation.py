"""Part of the Extraction Plant where generated metadata files are parsed
into raw dataclasses. 
"""


# ---Imports
import logging

from src.config import singleton
from src.beneficiations.parsers.parser import Parser
from src.utils.ingestibles import IngestibleDataclasses
from typing import Any

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Beneficiation(metaclass=singleton):
    """
    This class provides a means of beneficiating project, experiment, dataset and datafile files.
    It takes a list of files in the specified format and parses them into a dictionary of objects.
    """

    def __init__(
        self,
        parser: Parser,
    ) -> None:
        self.parser = Parser

    def beneficiate(
        self,
        beneficiation_data: dict[str, Any],
        ingestible_dataclasses: IngestibleDataclasses,
    ) -> IngestibleDataclasses:
        """Parse metadata files of a given file type into raw dataclasses

        Args:
            beneficiation_data (dict[str, Any]): Data that contains information about the dataclasses to parse
            ingestible_dataclasses (IngestibleDataclasses): A class that contains the raw datafiles, datasets, experiments, and projects.

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects.
        """
        logger.info("beneficiating")
        ing_dclasses = ingestible_dataclasses.copy()
        ing_dclasses_out = self.parser.parse(beneficiation_data, ing_dclasses)
        
        return ing_dclasses_out
