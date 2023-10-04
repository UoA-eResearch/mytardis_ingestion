# pylint: disable-all
# type: ignore
# This script needs a lot of refactoring so disable checks

"""Checks and selects the designated profile and loads the corresponding module
"""

# ---Imports
import importlib
import logging
from pathlib import Path
from types import ModuleType

from src.beneficiations.abstract_custom_beneficiation import AbstractCustomBeneficiation
from src.config.singleton import Singleton
from src.miners.abstract_custom_miner import AbstractCustomMiner
from src.prospectors.abstract_custom_prospector import AbstractCustomProspector

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

    def load_custom_prospector(
        self,
    ) -> AbstractProspector | None:
        module_pth = self.profile_module_str + custom_prspctr_lib
        try:
            custom_prospector: AbstractProspector = importlib.import_module(
                module_pth
            ).CustomProspector()
            return custom_prospector
        except Exception as e:
            logger.info(
                "AbstractCustomMiner not loaded, will be set to None. Below are the details:"
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
    ) -> AbstractCustomBeneficiation:
        module_pth = self.profile_module_str + custom_beneficiation_lib
        custom_beneficiation: AbstractCustomBeneficiation = importlib.import_module(
            module_pth
        ).CustomBeneficiation()
        return custom_beneficiation
