"""Checks and selects the designated profile and loads the corresponding module
"""

# ---Imports
import importlib
import logging
from pathlib import Path
from types import ModuleType

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# prof_base_lib = "src.profiles."
# custom_prspctr_lib = ".custom_prospector"
# custom_miner_lib = ".custom_miner"


# ---Code
class ProfileSelector:
    """
    This class is used to load a profile and the associated profile modules.

    Note that the profile path should return a python module.

    Attributes:
        profile (Path): The path to the profile to load.
    """

    def __init__(
        self,
        profile_path: Path,
    ) -> None:
        if not profile_path:
            logger.error("No profile set")
            raise ValueError("Error! Profile not set")
        self.profile_path = profile_path
        try:
            self.profile_module = importlib.import_module(profile_path.as_posix())
        except Exception as err:
            logger.error(err, exc_info=True)
            raise ImportError("Error loading profile module") from err

    def load_profile_module(  # pylint: disable=missing-function-docstring
        self,
    ) -> ModuleType:
        return self.profile_module

    def load_custom_prospector(
        self,
        custom_prospector: str = "custom_prospector",
    ) -> ModuleType:
        """Load a custom prospector

        Args:
            custom_prospector (str): The string representation of the prospector to load

        Returns:
            ModuleType: A reference to the loaded prospector python module
        """
        module_pth = self.profile_path / Path(custom_prospector)
        try:
            return importlib.import_module(module_pth.as_posix())
        except Exception as err:
            logger.error(err, exc_info=True)
            raise err

    def load_custom_miner(
        self,
        custom_miner: str,
    ) -> ModuleType:
        """Load a custom miner

        Args:
            custom_miner (str): The string representation of the miner to load

        Returns:
            ModuleType: A reference to the loaded miner python module
        """
        module_pth = self.profile_path / Path(custom_miner)
        try:
            return importlib.import_module(module_pth.as_posix())
        except Exception as err:
            logger.error(err, exc_info=True)
            raise err
