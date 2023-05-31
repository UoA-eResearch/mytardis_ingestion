"""Checks and selects the designated profile and loads the corresponding module
"""

# ---Imports
import importlib
import logging
from types import ModuleType

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
prof_base_lib = "src.profiles."
custom_prspctr_lib = ".custom_prospector"
custom_miner_lib = ".custom_miner"


# ---Code
class ProfileSelector:
    """
    This class is used to load a profile and the associated profile modules.

    Attributes:
        profile (str): The profile to load.
        profile_module_str (str): String representation of the full module path
        profile_module (ModuleType): The loaded module
    """

    def __init__(
        self,
        profile: str,
    ) -> None:
        if not profile:
            raise Exception("Error! Profile not set")
        self.profile = profile
        try:
            self.profile_module_str = prof_base_lib + profile
            self.profile_module = importlib.import_module(self.profile_module_str)
        except Exception as e:
            logger.error(e)
            raise Exception("Error loading profile module")

    def load_profile_module(
        self,
    ) -> ModuleType:
        return self.profile_module

    def load_custom_prospector(
        self,
    ) -> ModuleType:
        module_pth = self.profile_module_str + custom_prspctr_lib
        custom_prospector = importlib.import_module(module_pth)
        return custom_prospector

    def load_custom_miner(
        self,
    ) -> ModuleType:
        module_pth = self.profile_module_str + custom_miner_lib
        custom_miner = importlib.import_module(module_pth)
        return custom_miner
