#!/usr/bin/env python
# UoA MyTardis Harvester
# Written by Chris Seal <c.seal@auckland.ac.nz>

'''
Walk the staging directory tree looking for manifest files.
Using the manifest files, create a list of JSONs and datafiles
to process.

After processing manifests, create an instance of the myTardis 
uploader and process files that are ready for uploading.
'''

import logging
from harvester.helper import manifest as man
from harvester.ingestor import uploader as upld
import os
import json

logging.basicConfig(format = '%(asctime)s - %(name)s\n%(levelname)s - %(message)s\n=================================', datefmt='%d-%b-%y %H:%M:%S')

DEBUG = True
logger = logging.getLogger(__name__)
if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)
log_file = logging.FileHandler('mytardis.log')
logger.addHandler(log_file)

def readJSON(json_file):
    try:
        with open(json_file) as in_file:
            json_dict = json.load(in_file)
    except FileNotFoundError as fnferr:
        logger.error(fnferr)
        return False
    except Exception as err:
        logger.error(f'JSON file, {json_file}, unable to be read into dictionary')
        logger.error(err)
        return False
    else:
        logger.info(f'JSON file, {json_file}, sucessfully loaded')
        return json_dict

def _uppercase(obj):
    """ Make dictionary uppercase """
    if isinstance(obj, dict):
        return {k.upper():v for k, v in obj.items()}
    elif isinstance(obj, (list, set, tuple)):
        t = type(obj)
        return t(_uppercase(o) for o in obj)
    elif isinstance(obj, str):
        return obj.upper()
    else:
        return obj

class Harvester:
    
    def __init__(self, config_file, root_dir=None):
        if root_dir:
            self.root_dir = root_dir
        else:
            self.root_dir = os.getcwd()
        self.standard_blocks = {
            "experiment": "EXPT_JSONS",
            "dataset": "DATASET_JSONS",
            "datafile_json": "DATAFILE_JSONS",
            "datafiles": "DATAFILES"}
        self.config_file = config_file
        # Check if finished directory exists. If not create it
        self.finished_dir = os.path.join(root_dir,'../Finished_Processing')
        if not os.path.isdir(self.finished_dir):
            os.mkdir(self.finished_dir)
        logger.debug(self.config_file)

    def __find_manifests(self):
        '''
        Walk the directory tree starting at the top level and find manifest files.
        Currently these are identified by their file extension.

        Return a list of manifest files to be pocessed.
        '''
        known_extensions = ['manifest']

        manifests = []
        # Walk the directory tree
        os.chdir(self.root_dir)
        for (dirpath, dirnames, filenames) in os.walk(self.root_dir):
            for fname in filenames:
                extension = os.path.splitext(fname)[1][1:]
                if extension in known_extensions:
                    manifests.append(os.path.join(dirpath, fname))
        return manifests

    def __check_manifest(self, manifest):
        Validator = man.ManifestParser()
        path = os.path.dirname(manifest)
        manifest_name = os.path.basename(manifest)
        checked_manifest = Validator(manifest_name, path=path)
        return checked_manifest

    def __update_manifest(self, manifest):
        pass
    #TODO implement code to rewrite the updated dictionaries or delete if nothing left to process

    def __check_datafile_exists(self, datafile_json, manifest_dict, checked_dict):
        datafile_dict = readJSON(datafile_json)
        manifest_files = manifest_dict[self.standard_blocks['datafiles']]
        checked_files = checked_dict[self.standard_blocks['datafiles']]
        print(manifest_files)
        print(checked_files)
        return_dict = {}
        flg = True
        for key in datafile_dict.keys():
            print(key)
            if key == 'schema_namespace':
                continue
            if key not in manifest_files.keys():
                print('No key in manifest')
                return False
            #TODO Report an error here
            elif key not in checked_files.keys():
                print('No key in check')
                # We should never get here
                return False
            else:
                if flg:
                    flg = checked_files[key]
        return flg
            
            

    def __process_block(self, block, block_type, manifest_dict, checked_dict, Uploader):
        manifest_block = manifest_dict[block]
        checked_block = checked_dict[block]
        completed_keys = []
        for key in manifest_block.keys():
            try:
                check = checked_block[key]
            except KeyError as err:
                logger.error(f'Key, {key} not found in the checked_block')
                continue
            except Exception as err:
                logger.error(
                    f'Config file, {config_file}, unable to be read into dictionary')
                logger.error(err)
                sys.exit()
            if check:
                if block_type == 'experiment':
                    success = Uploader.create_experiment(key)
                elif block_type == 'dataset':
                    success = Uploader.create_dataset(key)
                elif block_type == 'datafile_json':
                    if self.__check_datafile_exists(key, manifest_dict, checked_dict):
                        success = Uploader.create_datafile(key)
                    else:
                        continue
                if not success:
                    if block_type == 'experiment':
                        logger.warn(f'Experiment JSON: {key} not properly processed')
                    elif block_type == 'dataset':
                        logger.warn(f'Dataset JSON: {key} not properly processed')
                    elif block_type == 'datafile_json':
                        logger.warn(f'Datafiles not fully uploaded')
                else:
                    file_name = os.path.basename(key)
                    file_path = os.path.join(self.finished_dir, file_name)
                    os.rename(key,file_path)
                    completed_keys.append(key)
            else:
                continue
        return completed_keys
    
    def process_manifest(self, manifest):
        path = os.path.dirname(manifest)
        manifest_name = os.path.basename(manifest)
        manifest_dict = _uppercase(toml.load(manifest))
        checked_dict = self.__check_manifest(manifest)
        Uploader = upld.MtUploader(self.config_file, path)
        # Process Experiments first
        block = self.standard_blocks['experiment']
        completed_keys = self.__process_block(block, 'experiment', manifest_dict, checked_dict, Uploader)
        for key in completed_keys:
            manifest_dict[block].pop(key)
        self.__update_manifest(manifest)
        # Next create datasets
        block = self.standard_blocks['dataset']
        completed_keys = self.__process_block(block, 'dataset', manifest_dict, checked_dict, Uploader)
        for key in completed_keys:
            manifest_dict[block].pop(key)
        self.__update_manifest(manifest)
        block = self.standard_blocks['datafile_json']
        completed_keys = self.__process_block(block, 'datafile_json', manifest_dict, checked_dict, Uploader)
        for key in completed_keys:
            manifest_dict[block].pop(key)
        self.__update_manifest(manifest)
        os.chdir(self.root_dir)

    def harvest(self):
        manifest_list = self.__find_manifests()
        for manifest in manifest_list:
            self.process_manifest(manifest)
