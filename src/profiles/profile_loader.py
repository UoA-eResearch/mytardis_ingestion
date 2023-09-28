"""Checks and selects the designated profile and loads the corresponding module
"""

# ---Imports
import importlib
import logging
from types import ModuleType

from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.config.singleton import Singleton
from src.miners.abstract_custom_miner import AbstractCustomMiner
from src.prospectors.prospector import AbstractCustomProspector

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
        profile_name: str,
    ) -> None:
        if not profile_name:
            logger.error("No profile set")
            raise ValueError("Error! Profile not set")
        self.profile_name = profile_name
        try:
            self.profile_module_str = prof_base_lib + self.profile_name
            self.profile_module = importlib.import_module(self.profile_module_str)
        except Exception as err:
            logger.error(err, exc_info=True)
            raise ImportError("Error loading profile module") from err

    def load_profile_module(
        self,
    ) -> ModuleType:
        return self.profile_module

    def load_custom_prospector(
        self,
        custom_prospector: str = "custom_prospector"
    ) -> AbstractCustomProspector | None:
        # module_pth = self.profile_module_str + '.' + custom_prspctr_lib
        try:
            custom_prospector: AbstractCustomProspector = importlib.import_module(
                name=('.'+custom_prospector),
                package=self.profile_module_str
            ).CustomProspector()
            return custom_prospector
        except Exception as e:
            logger.info(
                f"No custom prospector found for profile \"{self.profile_name}\" . Below are the details:"
            )
            logger.info(e)
            return None
    
    def load_custom_miner(
        self,
    ) -> AbstractCustomMiner | None:
        module_pth = self.profile_module_str + custom_miner_lib
        try:
            custom_miner: AbstractCustomMiner = importlib.import_module(
                module_pth
            ).CustomMiner()
            return custom_miner
        except Exception as e:
            logger.info(
                "AbstractCustomMiner not loaded, will be set to None. Below are the details:"
            )
            logger.info(e)
            return None

    def load_custom_beneficiation(
        self,
    ) -> AbstractCustomBeneficiation | None:
        module_pth = self.profile_module_str + custom_beneficiation_lib
        try:
            custom_beneficiation: AbstractCustomBeneficiation = importlib.import_module(
                module_pth
            ).CustomBeneficiation()
            return custom_beneficiation
        except Exception as e:
            logger.info(
                "AbstractCustomBeneficiation not loaded, will be set to None. Below are the details:"
            )
            logger.info(e)
            return None
