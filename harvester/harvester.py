# Abstract Harvester class as a base for the specialised Harvesters needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
from abc import ABC, abstractmethod
from ingestor import MyTardisUploader
from helper import readJSON
import logging
import os
import sys

logger = logging.getLogger(__name__)

class Harvester(ABC):

    def __init__(self,
                 config_dir):
        '''
        Initialise the class by pointing to a config directory.
        Creates an instace of a file uploader, a myTardis uploader and parser.
        In the abstract class the definition of the file uploader and parser are virtual since the specific class 
        will have specific parsers and a choice of file uploaders

        Inputs:
        =================================
        config_dir: the path to a directory containing specific config files

        Returns:
        =================================
        self.parser: An instance of the specific parser
        self.fileuploader: An instance of a specific file uploader
        self.mytardis: An instance of the myTardis uploader.
        '''
        os.chdir(config_dir)
        mytardis_config = readJSON('mytardis.json')
        parser_config = readJSON('parser.json')
        fileuploader_config = readJSON('uploader.json')
        self.mytardis = self.mytardis(mytardis_config)
        self.parser = self.parser(parser_config)
        self.fileuploader = self.fileuploader(fileuploader_config)

    def mytardis(self,
                 config_dict):
        try:
            ingestor = MyTardisUploader(config_dict)
        except Exception as err:
            logger.critical('Shutting down harvester')
            sys.exit()
        return ingestor
            
    @abstractmethod
    def fileuploader(self,
                     config_dict):
        pass

    @abstractmethod
    def parser(self,
               config_dict):
        pass
    
    @abstractmethod
    def harvest(self):
        pass
