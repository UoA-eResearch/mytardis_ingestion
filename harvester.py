# Harvester script that uses JSON files to instansiate a parser and a filehandler class

from .ingestor import MyTardisUploader
from .helper import readJSON, check_dictionary
from .helper import constants as CONST
from .mailserver import MailHandler
import logging
import importlib
from pathlib import Path
import os
import sys

logger = logging.getLogger(__name__)

'''The harvester script reads in json files that define the type of Parser and Filehandler to use, and provides the initialisation for these classes
#
This script then instansiates the classes and calls their create dictionaries and upload files to preprocess the data files ready for ingestion. One preprocessing is complete, the script calls the ingestor then updates the processed file list with successfully ingested files.
#
TODO: Use MyTardis to determine if a file has been successfully processed prior to ingestion to remove the necessity of using a processed file list.
'''

def harvest(harvester_json):
    # The harvester_json should contain the file paths to the ldap,
    # project_db, parser, ingestor and filehandler jsons.
    try:
        file_paths = readJSON(harvester_json)
    except Exception as error:
        # The function logs the error so raise to stop processing.
        raise
    # =================================
    # Define required keys for sanity checking
    # =================================
    required_keys = ['ldap',
                     'project_db',
                     'ingestor',
                     'parser',
                     'filehandler',
                     'mailhandler']
    ldap_keys = ['ldap_url'
                 'ldap_admin_user',
                 'ldap_admin_password',
                 'ldap_user_base']
    parser_keys = ['module',
                   'class_name',
                   'json_location']
    filehandler_keys = ['module',
                        'class_name',
                        'json_location']
    mailhandler_keys = ['server',
                        'port',
                        'username',
                        'password']
    #TODO: Check these as the url/api poss needs more info after PDB2
    project_db_keys = ['projectdb_api',
                       'projectdb_url']
    file_paths_check = check_dictionary(file_paths, required_keys)
    if not file_paths_check[0]:
        error_msg = f'Shutting down harvester due to incomplete harvester JSON. Missing keys {", ".join(file_paths_check[1])}'
        logger.error(error_msg)
        default_reciever = 'c.seal@auckland.ac.nz'
        if 'mailhandler' in file_paths.keys():
            try:
                mailhandler_dict = readJSON(file_paths['mailhandler'])
            except Exception as error:
                # The function logs the error so raise to stop processing.
                raise
            mailhandler_check = check_dictionary(mailhandler_dict, mailhandler_keys)
            mailhandler = MailHandler(mailhandler_dict)
            if 'recievers' in mailhandler_dict.keys():
                mailhandler.send_message('MyTardis Harvest Error',
                                         error_msg,
                                         reciever = \
                                         ", ".join(mailhandler_dict['recievers']))
            else:
                mailhandler.send_message('MyTardis Harvest Error',
                                         error_msg,
                                         reciever = default_reciever)
        raise Exception(error_msg)
    # =================================
    # If we have gotten to this point then there should at least be a file_path
    # to all of the relevant json files - load them in and do basic sanity checks
    # =================================
    try:
        ldap_dict = readJSON(file_paths['ldap'])
    except Exception as error:
        # The function logs the error so raise to stop processing.
        raise
    ldap_check = check_dictionary(ldap_dict, ldap_keys)
    try:
        project_db_dict = readJSON(file_paths['project_db'])
    except Exception as error:
        # The function logs the error so raise to stop processing.
        raise
    project_db_check = check_dictionary(project_db_dict)
    try:
        ingestor_dict = readJSON(file_paths['ingestor'])
    except Exception as error:
        # The function logs the error so raise to stop processing.
        raise
    try:
        mailhandler_dict = readJSON(file_paths['mailhandler'])
    except Exception as error:
        # The function logs the error so raise to stop processing.
        raise
    mailhandler_check = check_dictionary(mailhandler_dict, mailhandler_keys)
    try:
        filehandler_dict = readJSON(file_paths['filehandler'])
    except Exception as error:
        # The function logs the error so raise to stop processing.
        raise
    filehandler_check = check_dictionary(filehandler_dict, filehandler_keys)
    try:
        parser_dict = readJSON(file_paths['parser'])
    except Exception as error:
        # The function logs the error so raise to stop processing.
        raise
    parser_check = check_dictionary(parser_dict, parser_keys)
    # =================================
    # Check for errors in the error dictionary previously defined and
    # report them.
    # =================================
    errors = {}
    if not ldap_check[0]:
        errors['ldap'] = ldap_check[1]
    if not project_db_check[0]:
        errors['project_db'] = project_db_check[1]
    if not mailhandler_check[0]:
        errors['mailhandler'] = mailhandler_check[1]
    if not filehandler_check[0]:
        errors['filehandler'] = filehandler_check[1]
    if not parser_check[0]:
        errors['parser'] = parser_check[1]
    if errors != {}:
        error_msg = ''
        for key in errors.keys():
            error_msg += f'{key} is missing {", ".join(errors[key])} from keys\n'
        logger.error(error_msg)
        default_reciever = 'c.seal@auckland.ac.nz'
        if 'mailhandler' in file_paths.keys():
            try:
                mailhandler_dict = readJSON(file_paths['mailhandler'])
            except Exception as error:
                # The function logs the error so raise to stop processing.
                raise
            mailhandler_check = check_dictionary(mailhandler_dict, mailhandler_keys)
            mailhandler = MailHandler(mailhandler_dict)
            if 'recievers' in mailhandler_dict.keys():
                mailhandler.send_message(", ".join(mailhandler_dict['recievers']),
                                         'MyTardis Harvest Error',
                                         error_msg)
            else:
                mailhandler.send_message(default_reciever,
                                         'MyTardis Harvest Error',
                                         error_msg)
        raise Exception(error_msg)
    # =================================
    #
    # If we get to here then basic sanity checking has been done on all of the
    # loaded dictionaries - create class objects
    #
    # =================================
    # Initialise Mailhandler
    # =================================
    mailhandler = MailHander(mailhandler_dict)
    # =================================
    # Initialise Ingestor
    # =================================
    ingestor_dict.update(ldap_dict)
    ingestor_dict.update(project_db_dict)
    try:
        ingestor = MyTardisUploader(ingestor_dict)
    except Exception as error:
        mailhandler.send_message('MyTardis initialisation error',
                                 error.msg)
        # The function logs the error so raise to stop processing.
        raise
    # =================================
    # Initialise Filehandler
    # =================================
    filehandler_module = filehandler_dict['module']
    filehandler_class = filehandler_dict['class_name']
    try:
        filehandler_config = readJSON(filehandler_dict['json_location'])
    except Exception as error:
        mailhandler.send_message('MyTardis initialisation error',
                                 error.msg)
        # The function logs the error so raise to stop processing.
        raise
    try:
        module = importlib.import_module(filehandler_module)
        class_ = getattr(module, filehandler_class)
        filehandler = class_(filehandler_config)
    except Exception as error:
        mailhandler.send_message('MyTardis initialisation error',
                                 error.msg)
        # The function logs the error so raise to stop processing.
        logger.error(error.msg)
        raise
    # =================================
    # Initialise Parser
    # =================================
    parser_module = parser_dict['module']
    parser_class = parser_dict['class_name']
    try:
        parser_config = readJSON(parser_dict['json_location'])
    except Exception as error:
        mailhandler.send_message('MyTardis initialisation error',
                                 error.msg)
        # The function logs the error so raise to stop processing.
        raise
    Wif 'ldap' in parser_dict.keys():
        if parser_dict['ldap'] not False:
            parser_config.update(ldap_dict)
    if 'project_db' in parser_dict.keys():
        if parser_dict['project_db'] not False:
            parser_config.update(project_db_dict)
    try:
        module = importlib.import_module(parser_module)
        class_ = getattr(module, parser_class)
        parser = class_(parser_config)
    except Exception as error:
        mailhandler.send_message('MyTardis initialisation error',
                                 error.msg)
        # The function logs the error so raise to stop processing.
        logger.error(error.msg)
        raise
    # =================================
    #
    # Everything is initialised - begin parsing files
    #
    # =================================
    experiments = parser.create_experiment_dicts()
    datasets = parser.create_dataset_dicts()
    datafiles, checksum_digest = parser.create_datafile_dicts()
    # Use the ingestor to create the experiments
    for experiment in experiments:
        ingested_expts = ingestor.create_experiment(experiment)
    #TODO validation here
    # Use the ingestor to create the dataset
    for dataset in datasets:
        ingested_datasets = ingestor.create_dataset(dataset)
    #TODO validation here
    # Upload the files into storage and ingest
    for datafile in datafiles:
        upload = filehandler.upload_file(datafile, checksum_digest)
        ingested_datafiles = ingestor.create_datafile(datafile)
    #TODO Post ingestion stuff here
    
    
