from . import Harvester
import logging
from .parsers import SolarixParser
from .filehanders import SolarixFileHandler
import os
import sys

logger = logging.getLogger(__name__)

class SolarixHarvester(Harvester):

    def __init__(self,
                 config_dir)
    super().__init))(config_dir)

    def parser(self,
               config_dict):
        try:
            parser = SolarixParser(config_dict)
        except Exception as err:
            logger.critical(f'Shutting down harvester while initialising Parser, Error {err}')
            sys.exit()
        return parser

    def filehandler(self,
                    config_dict):
        try:
            filehandler = SolarixFileHandler(config_dict)
        except Exception as err:
            logger.critical(f'Shutting down harvester while initialising File handler, Error {err}')
            sys.exit()
        return filehandler

    def harvest(self):
        pass
