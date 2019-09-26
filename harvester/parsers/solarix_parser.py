# Solarix Directory Parser class 
#
# Provides basic structure details from the directory structure
# Abstract class as it must have the metadata attached some how
#
# Written by Chris Seal <c.seal@auckland.ac.nz>
#

__author__ = "Chris Seal <c.seal@auckland.ac.nz>"

from . import DirParser
import logging
import os
from xml.etree import ElementTree
import re
import requests
import json
from pathlib import Path
import ldap
import datetime

def get_immediate_subdirectories(parent_dir):
    return [name for name in os.listdir(parent_dir)
            if os.path.isdir(os.path.join(parent_dir, name))]

class SolarixParser(DirParser):

    def __init__(self,
                 config_dict):
        super().__init__(config_dict)
        self.project_db_key = config_dict['project_db_key']
        self.sub_dirs = self.build_dir_list(self.root_dir)
        self.default_user = config_dict['default_user']
        self.ldap_url = config_dict['ldap_url']
        self.ldap_admin_user = config_dict['ldap_admin_user']
        self.ldap_admin_password = config_dict['ldap_admin_password']
        self.ldap_user_attr_map = config_dict['ldap_user_attr_map']
        self.ldap_user_base = config_dict['ldap_user_base']
        self.projects = self.find_data(self.sub_dirs)
        self.default_user = config_dict['default_user']
        #TODO put proxies stuff here

    def check_project_db(self,
                         res_code):
        auth_code = f'Basic {self.project_db_key}'
        headers = {"Authorization": auth_code}
        url = f'https://projects.cer.auckland.ac.nz/projectdb/rest/projects/{res_code}'
        response = requests.get(url, headers=headers)
        return response

    def get_users_from_project(self,
                               response):
        users = []
        resp_dict = json.loads(response.text)
        l = ldap.initialize(self.ldap_url)
        l.protocol_version = ldap.VERSION3
        l.simple_bind_s(self.ldap_admin_user,
                        self.ldap_admin_password)
        for user in resp_dict['rpLinks']:
            user_email = user['researcher']['email']
            result = l.search_s(self.ldap_user_base,
                                ldap.SCOPE_SUBTREE,
                                "({0}={1})".format(self.ldap_user_attr_map['email'],
                                                   user_email))
            for e, r in result:
                if user['researcherRoleId'] == 1:
                    users.insert(0,r[self.ldap_user_attr_map["upi"]][0].decode('utf-8'))
                else:
                    users.append(r[self.ldap_user_attr_map["upi"]][0].decode('utf-8'))
        l.unbind_s()
        return users

    def get_name_and_abstract_from_project(self,
                                           response):
        resp_dict = json.loads(response.text)
        ret_dict = {"name": resp_dict['project']['name'],
                    "description": resp_dict['project']['description']}
        return ret_dict

    def extract_metadata(self,
                         file_path):
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
            # The world is broken log a failure
            pass
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

    def find_data(self,
                  directories=None):
        if not directories:
            directories = self.sub_dirs
        projects = {}
        for directory in directories:
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
            for part in directory.parts:
                if part.startswith('res'):
                    rescode = part.split('-')[0]
                    if re.match(r'res[a-z]{3}20[0-9]{6,}', rescode):
                        response = self.check_project_db(rescode)
                        if response.status_code < 300:
                            users = self.get_users_from_project(response)
                            project = self.get_name_and_abstract_from_project(response)
                            project['project_id'] = rescode
                elif re.match(r'\d{8}', part):
                    # it might be a date
                    try:
                        date = datetime.datetime.strptime(part, '%Y%m%d')
                    except ValueError as err:
                        pass
                    except Exception as err:
                        pass
                elif re.match(r'\d{6}', part):
                    print(part)
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
            projects[directory] = [users, project, expt, dataset, meta, data, metadata, date]
        return projects
                

    def create_datafile_dicts(self):
        datafiles = []
        for key in self.projects.keys():
            current_path = self.projects[key]
            if current_path[6]:
                datafile_dict = {}
                # This is metadata
                datafile_dict["file"] = current_path[2]["name"] + "_method.tar"
                datafile_dict["local_dir"] = key.relative_to(self.root_dir)
                datafile_dict["remote_dir"] = datafile_dict["local_dir"].parent
                datafile_dict["dataset_id"] = current_path[3]["dataset_id"]
                datafiles.append(datafile_dict)
            elif current_path[5]:
                for child in key.iterdir():
                    if child.is_file():
                        datafile_dict = {}
                        datafile_dict["file"] = child.name
                        datafile_dict["local_dir"] = key.relative_to(self.root_dir)
                        datafile_dict["remote_dir"] = datafile_dict["local_dir"]
                        datafile_dict["dataset_id"] = current_path[3]["dataset_id"]
                        datafiles.append(datafile_dict)
            if 'Ion Source' in current_path[4].keys():
                if current_path[4]['Ion Source'] == 'MALDI Imaging':
                    for child in key.parent.parent.iterdir():
                        print(child)
                        if child.is_file():
                            datafile_dict = {}
                            datafile_dict["file"] = child.name
                            datafile_dict["local_dir"] = key.parent.relative_to(self.root_dir)
                            datafile_dict["remote_dir"] = datafile_dict["local_dir"]
                            datafile_dict["dataset_id"] = current_path[3]["dataset_id"]
                            if datafile_dict not in datafiles:
                                datafiles.append(datafile_dict)
                    
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
                    'project_description': current_path[1]['description']}
        return experiments                         
