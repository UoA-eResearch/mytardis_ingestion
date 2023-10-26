# pylint: disable=C0301
"""Defines the methodology to convert the source metadata to a beneficiable
format on a path.
"""


# ---Imports
import copy
import logging
from typing import Any, Optional

from src.profiles import output_manager as om

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)  # set the level for which this logger will be printed.


# ---Code
class CustomMiner:
    """Profile-specific miner class

    Each profile has a custom miner class whose behaviour is based on the
    requirements of the researcher
    """

    def __init__(
        self,
    ) -> None:
        """Do not modify this method"""
        return None

    # pylint: disable=R0801
    def mine(
        self,
        path: str,
        recursive: bool,
        out_man: Optional[om.OutputManager] = None,
        options: Optional[dict[str, Any]] = None,
    ) -> om.OutputManager:
        """Mines metadata in a path

        Args:
            path (str): the path to inspect for metadata
            recursive (bool): True to inspect all subdirectories in path, False to inspect path only
            out_man (om.OutputManager): class which stores info of outputs of the pre-ingestion processes
            options (dict): extra options for the inspection

        Returns:
            om.OutputManager: output manager instance containing the outputs of the process
        """
        if not out_man:
            out_man = om.OutputManager()
        else:
            out_man = copy.deepcopy(out_man)

        _ = recursive  # Placeholder for the 'recursive' argument
        _ = path  # Placeholder for the 'path' argument
        _ = options  # Placeholder for the 'options' argument

        # Write the main inspection implementation here

        return out_man

    # Write rest of implementation here, use leading underscores for each method
