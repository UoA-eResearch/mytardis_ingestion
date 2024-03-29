# pylint: disable=R0801
# This needs a lot of refactoring so disable checks
# noqa
# nosec

"""Abstract parser module.

This is an abstract parser that defines the protocols
"""

# ---Imports
import logging
from abc import ABC, abstractmethod
from typing import Any

from src.extraction.manifest import IngestionManifest

# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class AbstractCustomBeneficiation(ABC):
    """A class to parse dataclass files into IngestionManifest objects."""

    def __init__(
        self,
    ) -> None:
        pass

    @abstractmethod
    def beneficiate(self, beneficiation_data: Any) -> IngestionManifest:
        """Parse metadata files of a given file type into raw dataclasses

        Args:
            beneficiation_data (Any): An object that contains a filepath or
            a structure of filepaths that contains metadata to parse into raw dataclasses
            ingestible_dataclasses (IngestionManifest): A class that contains the raw
            datafiles, datasets, experiments, and projects

        Returns:
            IngestionManifest: A class that contains the raw datafiles,
            datasets, experiments, and projects
        """
