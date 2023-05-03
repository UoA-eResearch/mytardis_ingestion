"""
"""


# ---Imports
import logging
import os
import sys

from pydantic import ValidationError
from src.beneficiations.beneficiation import Beneficiation
from src.helpers.config import ConfigFromEnv
from src.miners.miner import Miner
from src.prospectors.prospector import Prospector

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class PlantWithoutIME:
    def __init__(
        self,
        config: ConfigFromEnv = None,
        prospector: Prospector = None,
        miner: Miner = None,
        beneficiation: Beneficiation = None,
    ) -> None:
        if not config:
            try:
                config = ConfigFromEnv()
            except ValidationError as error:
                logger.error(
                    (
                        "An error occurred while validating the environment "
                        "configuration. Make sure all required variables are set "
                        "or pass your own configuration instance. Error: %s"
                    ),
                    error,
                )
                raise error
        
        propsector = (
            prospector 
            if prospector 
            else Prospector()
        )

        miner = (
            miner 
            if miner 
            else Miner()
        )

        beneficiation = (
            beneficiation 
            if beneficiation 
            else Beneficiation()
        )