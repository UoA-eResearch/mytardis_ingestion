"""Checks and selects the designated profile and loads the corresponding module
"""

# ---Imports
import importlib
import logging
from types import ModuleType

from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.miners.abstract_custom_miner import AbstractCustomMiner
from src.prospectors.abstract_custom_prospector import AbstractCustomProspector
from src.config.singleton import Singleton

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
prof_base_lib = "src.profiles."
custom_prspctr_lib = ".custom_prospector"
custom_miner_lib = ".custom_miner"
custom_beneficiation_lib = ".custom_beneficiation"


# ---Code
class ProfileLoader(metaclass=Singleton):
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
            raise Exception("Error loading profile module, profile not found")

    def load_profile_module(
        self,
    ) -> ModuleType:
        return self.profile_module

    def load_custom_prospector(
        self,
    ) -> AbstractCustomProspector|None:
        module_pth = self.profile_module_str + custom_prspctr_lib
        try:
            custom_prospector: AbstractCustomProspector = importlib.import_module(module_pth).CustomProspector()
            return custom_prospector
        except Exception as e:
            logger.info("AbstractCustomMiner not loaded, will be set to None. Below are the details:")
            logger.info(e)
            return None

    def load_custom_miner(
        self,
    ) -> AbstractCustomMiner|None:
        module_pth = self.profile_module_str + custom_miner_lib
        try:    
            custom_miner: AbstractCustomMiner = importlib.import_module(module_pth).CustomMiner()
            return custom_miner
        except Exception as e:
            logger.info("AbstractCustomMiner not loaded, will be set to None. Below are the details:")
            logger.info(e)
            return None

    def load_custom_beneficiation(
        self,
    ) -> AbstractCustomBeneficiation:
        module_pth = self.profile_module_str + custom_beneficiation_lib
        custom_beneficiation: AbstractCustomBeneficiation = importlib.import_module(module_pth).CustomBeneficiation()
        return custom_beneficiation
        