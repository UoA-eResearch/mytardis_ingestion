"""Defines the abstract interface to convert the source metadata to a beneficiable
format on a path.
"""


# ---Imports
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

from src.extraction_output_manager import output_manager as om

# ---Constants


# ---Code
class AbstractCustomMiner(ABC):
    """A class to parse dataclass files into IngestibleDataclasses objects."""

    def __init__(
        self,
    ) -> None:
        pass

    @abstractmethod
    def mine(
        self,
        path: Path,
        recursive: bool = True,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        """Mines metadata in a path

        Args:
            path (Path): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion
                processes

        Returns:
            om.OutputManager: output manager instance containing the outputs of the process
        """
        return out_man or om.OutputManager()
