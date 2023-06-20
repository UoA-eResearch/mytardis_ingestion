"""Performs the mining process

The mining process involves generating metadata files for MyTardis ingestion.
"""


# ---Imports
import logging

from pathlib import Path
from src.profiles import output_manager as om
from src.profiles import profile_selector
from typing import Optional

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class Miner:
    """Miner class for mining files according to a profile"""

    def __init__(
        self,
        profile: str,
    ) -> None:
        """Initialize the miner object.

        Args:
            profile (str): Name of the profile to use for mining.
        """
        self.profile_sel = profile_selector.ProfileSelector(profile)

    def mine_directory(
        self,
        path: Path,
        recursive: bool = True,
        out_man: Optional[om.OutputManager] = None,
    ) -> om.OutputManager:
        """Mine a directory according to the selected profile

        Args:
            path (Path): Path of the directory to mine.
            recursive (bool): Whether to recursively mine subdirectories.
            out_man (om.OutputManager): Output Manager to store the results.

        Returns:
            om.OutputManager: Output Manager with the mined results.
        """
        if not out_man:
            out_man = om.OutputManager()

        custom_miner = self.profile_sel.load_custom_miner()
        miner = custom_miner.CustomMiner()
        new_out_man : om.OutputManager = miner.mine(path, recursive, out_man)

        return new_out_man
