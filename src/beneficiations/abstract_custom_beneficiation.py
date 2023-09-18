# pylint: disable-all
# type: ignore
# This needs a lot of refactoring so disable checks

"""Abstract parser module.

This is an abstract parser that defines the protocols
"""

# ---Imports
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, List, Union

from src.extraction_output_manager.ingestibles import IngestibleDataclasses

# ---Constants
logger = logging.getLogger(__name__)


# ---Code
class AbstractCustomBeneficiation(ABC):
    """A class to parse dataclass files into IngestibleDataclasses objects."""

    def __init__(
        self,
    ) -> None:
        pass

    @abstractmethod
    def beneficiate(
        self,
        beneficiation_data: Any,
        ingestible_dclasses: IngestibleDataclasses,
    ) -> IngestibleDataclasses:
        """Parse metadata files of a given file type into raw dataclasses

        Args:
            beneficiation_data (Any): An object that contains a filepath or a structure of filepaths that contains metadata to parse into raw dataclasses
            ingestible_dataclasses (IngestibleDataclasses): A class that contains the raw datafiles, datasets, experiments, and projects

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects
        """
        pass
