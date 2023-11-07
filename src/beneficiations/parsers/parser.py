# pylint: disable=C0301, R0801
"""Abstract parser module.

This is an abstract parser that defines the protocols
"""

# ---Imports
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from src.utils.ingestibles import IngestibleDataclasses

# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class Parser(ABC):
    """A class to parse dataclass files into IngestibleDataclasses objects."""

    def __init__(
        self,
    ) -> None:
        pass

    @abstractmethod
    def parse(
        self,
        beneficiation_data: Dict[str, Any],
        ingestible_dclasses: IngestibleDataclasses,
    ) -> IngestibleDataclasses:
        """Parse metadata files of a given file type into raw dataclasses

        Args:
            beneficiation_data (dict[str, Any]): Data that contains information about the dataclasses to parse
            ingestible_dataclasses (IngestibleDataclasses): A class that contains the raw datafiles, datasets, experiments, and projects.

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects.
        """
