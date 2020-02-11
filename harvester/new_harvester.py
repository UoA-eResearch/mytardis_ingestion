# Abstract Harvester class as a base for the specialised Harvesters needed
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#
from abc import ABC, abstractmethod
from .ingestor import MyTardisUploader
from .helper import readJSON, calculate_checksum
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
        self.digest_file = Path(config['digest_file'])
        self.processed_file = Path(config['processed_file'])
        self.md5sum_executable = config_dict['md5sum_executable']
        if 's3_flag' in config_dict.keys():
            self.s3_flag = config_dict['s3_flag']
        else:
            self.s3_flag = False
        if 'sha512' in config_dict.keys():
            self.sha512_flag = config_dict['sha512_flag']
        else:
            self.sha512_flag = False
        self.blocksize = 1*CONST.GB
        self.ingestor = self.mytardis(mytardis_config)
        self.parser = self.parser(parser_config)
        self.filehandler = self.filehandler(filehandler_config)
        self.digest = self.read_digest(s3 = self.s3_flag, sha512 = self.sha512_flag)
        self.file_list = self.parser.get_file_list()

        # Needed for project DB Hack
        self.hack_dict = readJSON('hack.json')

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

    @abstractmethod
    def get_datafile_list(self):
        pass

    def read_processed(self):
        '''Function to read in a processed file and return a list
        of files that have been processed.

        Inputs:
        =================================
        None

        Returns:
        =================================
        processed_list, a list of Paths to files that have been successfully processed'''
        processed_list = []
        try:
            with open(self.processed_file, 'r') as f:
                for line in f:
                    processed_list.append(Path(line))
        except FileNotFoundError:
            logger.info(f'Unable to find file {self.processed_file}')
            return []
        except IOError:
            logger.warning(f'Unable to read file {self.processed_file}')
            return []
        except Exception as error:
            logger.error(f'An unhandled exception, {error}, was raised')
            raise
        return processed_list

    def read_digest(self, s3=False, sha512 = False):
        '''Function to read in a digest file, if one exits, and to create
        a dictionary in memory containing the MD5, S3-Etag and SHA512 hashes
        as required.

        Inputs:
        =================================
        s3 = False, a flag to show that the digest file contains a column of S3 Etags
        sha512 = False, a flag to show that the digest file contains a column of SHA512 hashes

        Returns:
        =================================
        digest_dict, a dictionary of hashes keyed to file name'''
        digest_dict = {}
        try:
            with open(self.digest_file, 'r') as f:
                for line in f:
                    data = line.split(',')
                    digest_dict[data[0]] = {}
                    digest_dict[data[0]]['md5'] = data[1]
                    if s3:
                        digest_dict[data[0]]['etag'] = data[2]
                        if sha512:
                            digest_dict[data[0]]['sha512'] = data[3]
                    elif sha512:
                        digest_dict[data[0]]['sha512'] = data[2]
        except FileNotFoundError:
            logger.info(f'Unable to find digest file {self.digest_file}')
            return {}
        except IOError:
            logger.warning(f'Unable to read digest file {self.digest_file}')
            return {}
        except Exception as error:
            logger.error(f'An unhandled exception, {error}, was raised')
            raise
        return digest_dict

    def build_digest(self, file_list, s3 = False, sha512 = False):
        '''Function to create a digest dictionary for files not in the digest file.
        Ignores already processed files and doesn't recalculate the checksums for 
        files for which there is a checksum in the digest file.

        Possible future update:
        =================================
        Add force flag to force recalculation of checksums in case of error

        Inputs:
        =================================
        file_list, a list of files to calculate checksums for
        s3, a flag that determines if the etag is calculated too
        sha512, flag to determine if the SHA512 hash is calculated

        Returns:
        =================================
        digest_dict, a dictionary of hashes keyed to file name'''
        digest_dict = self.digest_dict
        for file_name in file_list:
            if file_name in self.processed_list:
                continue
            elif file_name in digest_dict.keys():
                continue
            else:
                try:
                    checksums = calculate_checksum(file_name,
                                                   s3_flag = s2,
                                                   sha512_flag = sha512,
                                                   blocksize = self.blocksize)
                except Exception as error:
                    logger.error(f'Exception {error} occured during checksum calculations')
                    raise
                digest_dict[file_name]['md5'] = checksums[0]
                if s3:
                    digest_dict[file_name]['etag'] = checksums[1]
                    if sha512:
                        digest_dict[file_name]['sha512'] = checksums[2]
                elif sha512:
                    digest_dict[file_name]['sha512'] = checksums[1]
        self.digest_dict = digest_dict
        return True

    def mint_project_id(self):
        #TODO Code to access the RAiD service to mint a new RAiD if the project doesn't exist
        # Unitl then mint a UUID
        import uuid
        return uuid.uuid1()

    def enter_new_project_into_local_db(self, project_tup):
        # Note: This is a temporary solution until the projectDB can be properly leveraged
        # We need a local database of projects and project IDs
        import mysql.connector
        mydb = mysql.connector.connect(
            host = 'localhost',
            user = self.hack_dict['user'],
            passwd = self.hack_dict['password'],
            database = 'mytardis_local_project')
        mycursor = mydb.cursor()
        sql = "INSERT INTO projects (project_id, name) VALUES (%s, %s)"
        mycursor.execute(sql, poject_tup)
        mydb.commit()

    def get_project_id_by_secondary_id(self, secondary_id):
        # Note: This is a temporary solution until the projectDB can be properly leveraged
        # We need a local database of projects and project IDs
        import mysql.connector
        mydb = mysql.connector.connect(
            host = 'localhost',
            user = self.hack_dict['user'],
            passwd = self.hack_dict['password'],
            database = 'mytardis_local_project')
        mycursor = mydb.cursor()
        sql = "SELECT * FROM projects WHERE secondary_id = %s"
        secondary_id = (secondary_id,)
        mycursor.execute(sql, secondary_id)
        myresult = mycursor.fetchall()
        results = []
        for x in myresults:
            results.append(myresult)
        if len(results) > 1:
            raise ValueError(message=f'There are too many items with the name: {secondary_id[0]}.')
        elif len(results) < 1:
            return False
        else:
            return results[0]
