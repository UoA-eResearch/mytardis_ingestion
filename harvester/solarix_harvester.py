from . import Harvester
import logging
from .parsers import SolarixParser
from .filehanders import SolarixFileHandler
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

class SolarixHarvester(Harvester):

    def __init__(self,
                 config_dir,
                 filepath):
        super().__init__(config_dir)
        self.processed_list = self.read_processed_file_list(filepath)
        self.files_dict = {}
        
    def read_processed_file_list(self,
                                 filepath):
        with open(filepath, 'r') as f:
            for line in f:
                processed_list.append(Path(line))
        return processed_list
        
    def parser(self,
               config_dict):
        try:
            parser = SolarixParser(config_dict, self)
        except Exception as err:
            logger.critical(f'Shutting down harvester while initialising Parser, Error {err}')
            sys.exit()
        return parser

    def filehandler(self,
                    config_dict):
        try:
            filehandler = SolarixFileHandler(config_dict, self)
        except Exception as err:
            logger.critical(f'Shutting down harvester while initialising File handler, Error {err}')
            sys.exit()
        return filehandler

    def harvest(self):
        pass
