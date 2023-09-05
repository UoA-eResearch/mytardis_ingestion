"""Defines the abstract interface to inspect metadata and perform related checks on a path.
"""


# ---Imports
import copy
import logging
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any, Dict, Optional

from src.extraction_output_manager import output_manager as om
from src.profiles import profile_consts as pc

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class AbstractCustomProspector:
    """Profile-specific prospector class

    Each profile has a custom prospector whose behaviour is based on the
    requirements of the researcher
    """

    def __init__(
        self,
    ) -> None:
        """Do not modify this method"""
        return None

    @abstractmethod
    def prospect(
        self,
        path: Path,
        recursive: bool,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        """Prospects metadata in a path

        Args:
            path (Path): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion processes

        Returns:
            om.OutputManager: output manager instance containing the outputs of the process
        """
        pass
