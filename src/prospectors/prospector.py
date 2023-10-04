"""Performs the prospecting process

Prospecting checks files and metadata files for any potential issues.

Specific prospector classes need to be subclassed from the abstract Prospector class
"""

import logging

# ---Imports
from abc import ABC, abstractmethod
from pathlib import Path

from src.extraction_output_manager.output_manager import OutputManager

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class AbstractProspector(ABC):
    """Abstract base class for the Prospector is used to prospect a given directory for files.

    Attributes:
        profile_sel (profile_selector.ProfileSelector): selected profile module
    """

    @abstractmethod
    def prospect(
        self,
        path: Path,
        recursive: bool,
        out_man: OutputManager,
    ) -> None:
        """Prospects metadata in a path

    def prospect_directory(
        self,
        path: str,
        recursive: bool = True,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        """
        This method prospect_directory is used to prospect a given directory for files.

        Parameters:
            path (str): Path to directory.
            recursive (bool): Whether to recursively search for files.
            out_man (om.OutputManager): Output manager instance.

        Returns:
            OutputManager: output manager instance containing the outputs of the process
        """
        return None
