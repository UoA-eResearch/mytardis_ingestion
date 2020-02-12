# Abstract Harvester class as a base for the specialised Harvesters needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
from abc import ABC, abstractmethod
from .ingestor import MyTardisUploader
from .helper import readJSON
from .helper import constants as CONST
import logging
import os
import sys
import hashlib
import subprocess
from pathlib import Path

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
        ldap_keys = ['ldap_url'
                     'ldap_admin_user',
                     'ldap_admin_password',
                     'ldap_user_base']
        project_db_keys = ['projectdb_api',
                           'projectdb_url']
        harvester_keys = ['root_dir']
        os.chdir(config_dir)
        config_dict = readJSON('harvester.json')
        ldap_dict = readJSON('../ldap.json')
        project_db_dict = readJSON('../cerproject.json')
        mytardis_config = readJSON('mytardis.json')
        parser_config = readJSON('parser.json')
        filehandler_config = readJSON('filehandler.json')
        self.ldap_url = ldap_dict['ldap_url']
        self.ldap_admin_user = ldap_dict['ldap_admin_user']
        self.ldap_admin_password = ldap_dict['ldap_admin_password']
        self.ldap_user_attr_map = ldap_dict['ldap_user_attr_map']
        self.ldap_user_base = ldap_dict['ldap_user_base']
        self.projectdb_url = project_db_dict['projectdb_url']
        self.projectdb_key = project_db_dict['projectdb_api']
        if 'proxies' in config_dict.keys():
            self.proxies = config_dict['proxies']
        else:
            self.proxies = None
        self.root_dir = Path(config_dict['root_dir'])
        harvester = self
        self.md5sum_executable = config_dict['md5sum_executable']
        self.subprocess_size_threshold = 100*CONST.MB
        self.ingestor = self.mytardis(mytardis_config, harvester)
        self.parser = self.parser(parser_config, harvester)
        self.filehandler = self.filehandler(filehandler_config, harvester)

    def mytardis(self,
                 config_dict,
                 harvester):
        try:
            ingestor = MyTardisUploader(config_dict,harvester)
        except Exception as err:
            logger.critical(f'Shutting down harvester while initialising MyTardis, Error: {err}')
            sys.exit()
        return ingestor
            
    @abstractmethod
    def filehandler(self,
                    config_dict,
                    harvester):
        pass

    @abstractmethod
    def parser(self,
               config_dict,
               harvester):
        # Add needed values to the parser_config dictionary before handing it off
        # For example completed files/csvs
        pass
    
    @abstractmethod
    def harvest(self):
        pass