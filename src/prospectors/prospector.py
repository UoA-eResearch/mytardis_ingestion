"""Performs the prospecting process

Prospecting checks files and metadata files for any potential issues.

Specific prospector classes need to be subclassed from the abstract Prospector class
"""

import logging

# ---Imports
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Tuple

from src.extraction_output_manager.output_manager import OutputManager
from src.prospectors.common_file_checks import perform_common_file_checks

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

        Args:
            path (Path): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion processes

        Returns:
            OutputManager: output manager instance containing the outputs of the process
        """
        return None
