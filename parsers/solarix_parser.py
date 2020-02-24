# Solarix Directory Parser class 
#
# Provides basic structure details from the directory structure
# Abstract class as it must have the metadata attached some how
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

__author__ = "Chris Seal <c.seal@auckland.ac.nz>"

from .parser import Parser
import logging
import os
from xml.etree import ElementTree
import requests
import json
from pathlib import Path
import datetime
import pytz
import mimetypes
from ..helper import calculate_checksum, most_probable_date, research_code_from_string, readJSON
from ..helper import ProjectDBFactory, RAiDFactory
from ..helper import get_user_from_email, get_user_from_upi
from ..helper import zip_directory
from decouple import Config, RepositoryEnv
import dateutil.parser as dparse

DEVELOPMENT = True

class SolarixParser(Parser):

    def __init__(self,
                 global_config_file_path,
                 local_config_file_path):
        # global_config holds environment variables that don't change often such as LDAP parameters and project_db stuff
        global_config = Config(RepositoryEnv(global_config_file_path))
        # local_config holds the details about how this particular set of data should be handled
        local_config = Config(RepositoryEnv(local_config_file_path))
        self.staging_dir = Path(local_config('FILEHANDLER_STAGING_ROOT'))
        self.default_user = local_config('MYTARDIS_FACILITY_MANAGER')
        self.ldap_dict = {}
        self.ldap_dict['url'] = global_config('LDAP_URL')
        self.ldap_dict['user_attr_map'] = {'first_name': global_config('LDAP_USER_ATTR_MAP_FIRST_NAME'),
                                           'last_name': global_config('LDAP_USER_ATTR_MAP_LAST_NAME'),
                                           'email': global_config('LDAP_USER_ATTR_MAP_EMAIL'),
                                           'upi': global_config('LDAP_USER_ATTR_MAP_UPI')}
        self.ldap_dict['admin_user'] = global_config('LDAP_ADMIN_USER')
        self.ldap_dict['admin_password'] = global_config('LDAP_ADMIN_PASSWORD')
        self.ldap_dict['user_base'] = global_config('LDAP_USER_BASE')
        self.project_db_factory = ProjectDBFactory(global_config_file_path)
        self.raid_factory = RAiDFactory(global_config_file_path)
        self.m_dirs = self.walk_dir_tree()
        # Temporary file to hold minted raids so we don't end up hitting the minting service too often
        # during development.
        self.proj_raid_list_file = global_config('DEV_PROJ_RAID_FILE', default=None)
        if self.proj_raid_list_file:
            self.proj_raid_list = readJSON(self.proj_raid_list_file)
        self.expt_raid_list_file = global_config('DEV_EXPT_RAID_FILE', default=None)
        if self.expt_raid_list_file:
            self.expt_raid_list = readJSON(self.expt_raid_list_file)
        self.dset_raid_list_file = global_config('DEV_DSET_RAID_FILE', default=None)
        if self.dset_raid_list_file:
            self.dset_raid_list = readJSON(self.dset_raid_list_file)
        # TODO: Rework this self.projects = self.find_data(self.sub_dirs)
        self.processed = self.__process_m_dirs(self.m_dirs)
        self.local_tz = pytz.timezone('Pacific/Auckland')
        self.utc = pytz.timezone('UTC')


    def get_users_from_project_code(self,
                                    code,
                                    roles=['1','2','3']):
        project_db_id = self.project_db_factory.get_project_id_from_code(code)
        people_ids = self.project_db_factory.get_people_ids_from_project(project_db_id,
                                                                         roles)
        upis = []
        for person in people_ids:
            email = self.project_db_factory.get_email_from_person_project_db_id(person)
            user = get_user_from_email(self.ldap_dict,
                                       email)
            upis.append(user['username'])
        return upis

    def get_user_from_ldap(self,
                           upi):
        '''Test to see if the upi is valid by returning user from LDAP'''
        #TODO Error handling
        user = get_user_from_upi(upi)
        if not user:
            return None
        else:
            return upi

    def __extract_metadata(self,
                           file_path):
        # Refactor to make more generic but this will do for now
        key_list = ['API_Polarity',
                    'MW_low',
                    'MW_high',
                    'AZURA_Enable',
                    'LC_mode',
                    'API_SourceType',
                    'Q1DC',
                    'Q1Mass',
                    'Q1Res',
                    'Q1CID',
                    'Q1_Frag_Energy',
                    'ECD']
        param_dict = {}
        tree = ElementTree.parse(file_path)
        root = tree.getroot()
        method = root.find('methodmetadata')
        primary = method.find('primarykey')
        samplenotes = primary.find('samplename')
        comments = primary.find('sampledescription')
        paramlist = root.find('paramlist')
        params = paramlist.iterfind('param')
        for param in params:
            if param.attrib['name'] in key_list:
                value = param.find('value')    
                param_dict[param.attrib['name']] = value.text
        meta = {}
        meta['Sample Notes'] = samplenotes.text
        meta['Comments'] = comments.text
        if param_dict['API_Polarity'] == '0':
            meta['Polarity'] = 'Positive'
        elif param_dict['API_Polarity'] == '1':
            meta['Polarity'] = 'Negative'
        else:
            logger.warning(f'Unable to find Polarity for {file_path}')
        if 'MW_low' in param_dict.keys() and 'MW_high' in param_dict.keys():
            low = int(round(float(param_dict['MW_low']),0)) #TODO Error handling
            high = int(round(float(param_dict['MW_high']),0))
            mz_range = f'{low} - {high}'
            meta['m/z Range'] = mz_range
        if param_dict['AZURA_Enable'] == '1':
            if param_dict['LC_mode'] == '0':
                meta['Ion Source'] = 'MALDI'
            elif param_dict['LC_mode'] == '6':
                meta['Ion Source'] = 'MALDI Imaging'
            else:
                #log an issue and move on
                pass
        elif param_dict['AZURA_Enable'] == '0':
            if param_dict['API_SourceType'] == '1':
                meta['Ion Source'] = 'ESI'
            elif param_dict['API_SourceType'] == '2':
                meta['Ion Source'] = 'APCI'
            elif param_dict['API_SourceType'] == '3':
                meta['Ion Source'] = 'NanoESI'
            elif param_dict['API_SourceType'] == '4':
                meta['Ion Source'] = 'NanoESI'
            elif param_dict['API_SourceType'] == '5':
                meta['Ion Source'] = 'APPI'
            elif param_dict['API_SourceType'] == '11':
                meta['Ion Source'] = 'CaptiveSpray'
            else:
                # something is wrong - log it
                pass
        else:
            # Ahhhh more errors - log them
            pass
        if param_dict['Q1DC'] == '0':
            meta['Isolate'] = 'No'
        elif param_dict['Q1DC'] == '1':
            meta['Isolate'] = 'Yes'
            meta['Isolation Mass'] = param_dict['Q1Mass']
            meta['Isolation Window'] = param_dict['Q1Res']
        else:
            # errors so log
            pass
        if param_dict['Q1CID'] == '0':
            meta['CID'] = 'No'
        elif param_dict['Q1CID'] == '1':
            meta['CID'] = 'Yes'
            meta['CID Energy'] = param_dict['Q1_Frag_Energy']
        else:
            # Its broken you know what to do
            pass
        if param_dict['ECD'] == '0':
            meta['ECD'] = 'No'
        elif param_dict['ECD'] =='1':
            meta['ECD'] = 'Yes'
        return meta

    def __get_owners_and_users_from_project_code(self,
                                                 code,
                                                 owners,
                                                 users):
        try:
            project_db_id = self.project_db_factory.get_project_id_from_code(code)
        except Exception as error:
            code = None
            pass # Bail out of adding users by project if we can't get the id
        else:
            people_ids = self.project_db_factory.get_people_from_project(project_db_id,
                                                                         roles = [1,2])
            # Person role 1 & 2 are the project owner and supervisor so get ownership rights
            for person in people_ids:
                email = self.project_db_factory.get_email_from_person_project_db_id(person)
                upi = get_user_from_email(self.ldap_dict,
                                          email)
                if upi in owners:
                    continue # Only add new people
                else:
                    owners.append(upi)
            people_ids = self.project_db_factory.get_people_from_project(project_db_id,
                                                                         roles = [3])
            # Person role 3 is a project member and gets user rights
            for person in people_ids:
                email = self.project_db_factory.get_email_from_person_project_db_id(person)
                upi = get_user_from_email(self.ldap_dict,
                                          email)
                if upi in users:
                    continue # Only add new people
                else:
                    users.append(upi)
        return (owners, users, project_db_id)

    def __get_basic_info(self,
                         m_directory):
        date = None
        code = None
        project_db_id = None
        owners = [self.default_user]
        users = []
        for part in m_directory.parts:
            test_date = most_probable_date(part)
            if test_date:
                date = test_date # This will ensure that the date closest to the m_directory will be used
            rescode = research_code_from_string(part)
            if rescode:
                code = rescode # Same logic as above
            upi = upi_from_string(part)
            if upi:
                if upi == self.default_user:
                    continue # only add the user if they are someone new
                if len(owners) == 1:
                    owners.append(upi)
                else:
                    owners[1] = upi # Ditto two previous - note this writes over the second position in the list
        d_directory = m_directory.parent
        sample_name = d_directory[:-2]
        if code:
            owners, users, project_db_id = self.__get_owners_and_users_from_project_code(code,
                                                                                         owners,
                                                                                         users)
        return (date, project_db_id, owners, users, d_directory, sample_name)

    def __imaging_process(self,
                          m_directory):
        pass
    
    def __non_imaging_process(self,
                              m_directory):
        # Look for a date stamp, upi and/or research code
        date, project_db_id, owners, users, d_directory, sample_name = self.__get_basic_info(m_directory)
        # project_db_id is kept for future proofing
        zip_name = sample_name + '.zip'
        zip_directory(d_directory,
                      zip_name)
        ret_dict = {'date': date,
                    'project_db_id': project_db_id,
                    'owners': owners,
                    'users': users,
                    'd_directory': d_directory,
                    'sample_name': sample_name,
                    'zip_name': zip_name}
        return ret_dict
        
    def walk_dir_tree(self,
                      directory = None): # directory is a Path object
        if not directory:
            directory = self.staging_dir
        m_list = []
        for dirname, subdirs, files in os.walk(directory):
            if dirname[-2:] == '.m':
                m_list.append(Path(dirname))
        return m_list   

    def __process_m_dirs(self,
                         m_dir_list):
        for m_dir in m_dir_list:
            processed_dict = None
            method_file = m_dir / 'apexAcquisition.method'
            if method_file.is_file():
                meta_dict = self.__extract_metadata(method_file)
            if meta_dict['Ion Source'] == 'MALDI Imaging':
                processed_dict = self.__imaging_process(m_dir)
            else:
                processed_dict = self.__non_imaging_process(m_dir)
            if processed_dict:
                if processed_dict['project_db_id']:
                    project_db_url = f'https://not-currently-implemented/{processed_dict["project_db_id"]}'
                    project_title, project_description, project_creation_date = \
                        self.project_db_factory.get_name_and_description_by_project_id(processed_dict['project_db_id'])
                    project_creation_date = datetime.datetime.strptime(project_creation_date, '%Y-%m%dT%H:%M:%SZ')
                    project_creation_date = self.utc.localize(project_creation_date)
                    project_creation_date = project_creation_date.astimezone(self.local_tz)
                else:
                    project_db_url = f'https://not-currently-implemented/No-Project'
                    project_title = 'No Project'
                    project_description = 'Empty project for no identifiers'
                    project_creation_date = datetime.datetime(2000, 1, 1)
                    project_creation_date = self.local_tz.localize(project_creation_date)
                project_raid = None
                if DEVELOPMENT:
                    if project_db_url in self.proj_raid_list.keys():
                        project_raid = self.proj_raid_list[project_db_url]
                if not project_raid:
                    response = self.raid_factory.mint_project_raid(project_title,
                                                                   project_description,
                                                                   project_db_url,
                                                                   project_startdate=project_creation_date)
                    project_raid = response.json()['handle']
                    if DEVELOPMENT:
                        self.proj_raid_list[project_db_url] = project_raid
                

    def find_data(self,
                  directories=None): # Too complex - simplify
        if not directories:
            directories = self.sub_dirs
        projects = {}
        for directory in directories:
            if directory in self.harvester.processed_list:
                continue
            date = None
            users = []
            project = {'project_id': "No_Project",
                       'name': 'No_Project',
                       'description': 'No description'}
            expt = {}
            dataset = {}
            meta = {}
            data = False
            metadata = False
            m_dirs = []
            for part in directory.parts:
                
                elif re.match(r'\d{8}', part):
                    # it might be a date
                    try:
                        date = datetime.datetime.strptime(part, '%Y%m%d')
                    except ValueError as err:
                        pass
                    except Exception as err:
                        pass
                elif re.match(r'\d{6}', part):
                    try:
                        date = datetime.datetime.strptime(part, "%d%m%y")
                    except ValueError as err:
                        pass
                    except Exception as err:
                        pass
                elif re.match(r'[a-z,A-Z]{4}\d{3}-FTICRMS', part):
                    users = [part.split('-')[0]]
                    project = {"name": part,
                               "description": "No description",
                               "project_id": part}
                elif re.match(r'\w*_\d{6}.d', part):
                    part_list = part.split('_')
                    del part_list[-1]
                    name = '_'
                    expt = {"name": name.join(part_list)}
                    dataset = {"name": part[:-2]}
                elif part[-2:] == ".d":
                    expt = {"name": part[:-2]}
                    dataset = {"name": part[:-2]}
                elif part[-2:] == ".m":
                    m_dirs.append(directory)
                    file_path = directory.joinpath("apexAcquisition.method")
                    if os.path.isfile(file_path):
                        meta = self.extract_metadata(file_path)
            try:
                expt_name = expt["name"]
                expt['internal_id'] = f'{project["project_id"]}-{expt_name}'
            except Exception as err:
                pass
            try:
                dataset_name = dataset["name"]
                dataset['dataset_id'] = f'{expt["internal_id"]}-{dataset_name}'
            except Exception as err:
                pass
            if directory.suffix == ".d":
                data = True
            elif directory.suffix == ".m":
                data = True
                metadata = True
            projects[directory] = [users, project, expt, dataset, meta, data, metadata, date, m_dirs]
        return projects


    

    def create_datafile_dicts(self):
        datafiles = []
        for key in self.projects.keys():
            current_path = self.projects[key]
            if current_path[6]:
                datafile_dict = {}
                datafile_dict['schema_namespace'] = self.datafile_schema
                # This is metadata
                datafile_dict["file"] = current_path[2]["name"] + "_method.tar.gz"
                datafile_dict["local_dir"] = key.relative_to(self.harvester.root_dir)
                datafile_dict["remote_dir"] = datafile_dict["local_dir"].parent
                datafile_dict["dataset_id"] = current_path[3]["dataset_id"]
                datafile_dict['md5sum'] = calculate_checksum(os.path.join(self.harvester.filehandler.staging_dir, datafile_dict['remote_dir']),
                                                             datafile_dict["file"],
                                                             md5sum_executable = self.harvester.md5sum_executable,
                                                             subprocess_size_threshold = self.harvester.subprocess_size_threshold)
                datafile_dict['mimetype'] = mimetypes.guess_type(child.name)[0]
                datafile_dict['size'] = os.path.getsize(os.path.join(self.harvester.filehandler.staging_dir,
                                                                     datafile_dict['remote_dir'],
                                                                     datafile_dict["file"]))
                datafiles.append(datafile_dict)
            elif current_path[5]:
                for child in key.iterdir():
                    if child.is_file():
                        datafile_dict = {}
                        datafile_dict['schema_namespace'] = self.datafile_schema
                        datafile_dict["file"] = child.name
                        datafile_dict["local_dir"] = key.relative_to(self.harvester.root_dir)
                        datafile_dict["remote_dir"] = datafile_dict["local_dir"]
                        datafile_dict["dataset_id"] = current_path[3]["dataset_id"]
                        datafile_dict['md5sum'] = calculate_checksum(key,
                                                                     datafile_dict["file"],
                                                                     md5sum_executable = self.harvester.md5sum_executable,
                                                                     subprocess_size_threshold = self.harvester.subprocess_size_threshold)
                        datafile_dict['mimetype'] = mimetypes.guess_type(child.name)[0]
                        datafile_dict['size'] = os.path.getsize(os.path.join(key,datafile_dict["file"]))
                        datafiles.append(datafile_dict)
            if 'Ion Source' in current_path[4].keys():
                if current_path[4]['Ion Source'] == 'MALDI Imaging':
                    for child in key.iterdir():
                        datafile_dict = {}
                        datafile_dict['schema_namespace'] = self.datafile_schema
                        if child.is_file():
                            datafile_dict["file"] = child.name
                            datafile_dict["local_dir"] = key.relative_to(self.harvester.root_dir)
                            datafile_dict["remote_dir"] = datafile_dict["local_dir"]
                            datafile_dict["dataset_id"] = current_path[3]["dataset_id"]
                            datafile_dict['md5sum'] = calculate_checksum(key,
                                                                     datafile_dict["file"],
                                                                     md5sum_executable = self.harvester.md5sum_executable,
                                                                     subprocess_size_threshold = self.harvester.subprocess_size_threshold)
                            datafile_dict['mimetype'] = mimetypes.guess_type(child.name)[0]
                            datafile_dict['size'] = os.path.getsize(os.path.join(key,datafile_dict["file"]))
                            datafiles.append(datafile_dict)
                    for child in key.parent.parent.iterdir():
                        if child.is_file():
                            datafile_dict = {}
                            datafile_dict["file"] = child.name
                            datafile_dict["local_dir"] = key.parent.parent.relative_to(self.harvester.root_dir)
                            datafile_dict["remote_dir"] = datafile_dict["local_dir"]
                            datafile_dict["dataset_id"] = current_path[3]["dataset_id"]
                            datafile_dict['md5sum'] = calculate_checksum(key.parent.parent,
                                                                         child.name,
                                                                         md5sum_executable = self.harvester.md5sum_executable,
                                                                         subprocess_size_threshold = self.harvester.subprocess_size_threshold)
                            datafile_dict['mimetype'] = mimetypes.guess_type(child.name)[0]
                            datafile_dict['size'] = os.path.getsize(os.path.join(key.parent.parent,datafile_dict["file"]))
                        if datafile_dict not in datafiles:
                            datafiles.append(datafile_dict)
        for datafile in datafiles:
            if datafile['local_dir'] not in self.harvester.files_dict.keys():
                self.harvester.files_dict[datafile['local_dir']] = []
            self.harvester.files_dict[datafile['local_dir']].append(datafile['file'])
        return datafiles

    def create_dataset_dicts(self):
        datasets = {}
        for key in self.projects.keys():
            current_path = self.projects[key]
            if current_path[5] and not current_path[6]: # This has data but not metadata
                if current_path[3]['dataset_id'] not in datasets.keys():
                    datasets[current_path[3]['dataset_id']] = {
                        'internal_id':current_path[2]['internal_id'],
                        'dataset_id':current_path[3]['dataset_id'],
                        'description': current_path[3]['name']}
                elif 'dataset_id' not in datasets[current_path[3]['dataset_id']].keys():
                    datasets[current_path[3]['dataset_id']]['internal_id'] = current_path[2]['internal_id']
                    datasets[current_path[3]['dataset_id']]['dataset_id'] = current_path[3]['dataset_id']
                    datasets[current_path[3]['dataset_id']]['description'] = current_path[3]['name']
                else:
                    # Log and error as we have a double up
                    pass
            elif current_path[6]:
                # This is metadata
                if current_path[3]['dataset_id'] not in datasets.keys():
                    datasets[current_path[3]['dataset_id']] = {}
                datasets[current_path[3]['dataset_id']].update(current_path[4])
                datasets[current_path[3]['dataset_id']]['schema_namespace'] = self.dataset_schema
        return datasets

    def create_experiment_dicts(self):
        experiments = {}
        for key in self.projects.keys():
            current_path = self.projects[key]
            if current_path[2] == {}:
                continue
            if current_path[2]['internal_id'] not in experiments.keys():
                if self.default_user[0] not in current_path[0]:
                    users = current_path[0]
                    if users:
                        owners = self.default_user + [users.pop(0)]
                    else:
                        owners = self.default_user
                else:
                    if current_path[0].index(self.default_user[0]) == 0:
                        owners = self.default_user
                    else:
                        owners = self.default_user + [current_path[0].pop(0)]
                    current_path[0].remove(self.default_user[0])
                    users = current_path[0]
                if not users:
                    users = None
                experiments[current_path[2]['internal_id']] = {
                    'internal_id': current_path[2]['internal_id'],
                    'title': current_path[2]['name'],
                    'owners': owners,
                    'users': users,
                    'project_id': current_path[1]['project_id'],
                    'project_name': current_path[1]['name'],
                    'project_description': current_path[1]['description'],
                    'schema_namespace': self.expt_schema}
        return experiments




    
