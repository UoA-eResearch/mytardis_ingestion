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

def get_immediate_subdirectories(parent_dir):
    return [name for name in os.listdir(parent_dir)
            if os.path.isdir(os.path.join(parent_dir, name))]

class SolarixParser(DirParser):

    def __init__(self,
                 config_dict):
        super().__init__(config_dict)
        self.project_db_key = config_dict['project_db_key']
        #TODO put proxies stuff here

    def create_datafile_dicts(self):
        pass

    def create_dataset_dicts(self):
        pass

    def create_experiment_dicts(self):
        pass

    def check_project_db(self,
                         res_code):
        auth_code = f'Basic {self.project_db_key}'
        headers = {"Authorization": auth_code}
        url = f'https://projects.cer.auckland.ac.nz/projectdb/rest/projects/{res_code}'
        response = requests.get(url, headers=headers)
        return response
        
    def walk_directory(self):
        '''Use os.walk to get the subdirectories under the root directory
        and use these to locate data and metadata files'''

        m_dirs = []
        for path, directory, filename in os.walk(self.root_dir):
            if 'apexAcquisition.method' in filename:
                m_dirs.append(path)
            cur_dirname = os.path.split(path)[-1]
            if cur_dirname.startswith('res'):
                # This is probably a research project code so look up the project db
                res_code = cur_dirname.split('-')[0]
                if re.match(r'res[a-z]{3}20[0-9]{6,}', res_code):
                    response = self.check_project_db(res_code)
                    project_dict = json.loads(response.text)
                    print(project_dict)
                    print(project_dict['project'])
                    print(project_dict['project']['name'])
        print(m_dirs)
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
        for m_dir in m_dirs:
            print(m_dir)
            if re.match(r'[a-z,A-Z,0-9,/]*res[a-z]{3}20[0-9]{6,}', m_dir):
                print('Match')
            else:
                print(':(')
        for m_dir in m_dirs:
            tree = ElementTree.parse(os.path.join(m_dir,'apexAcquisition.method'))
            root = tree.getroot()
            method = root.find('methodmetadata')
            primary = method.find('primarykey')
            samplenotes = primary.find('samplename')
            comments = primary.find('sampledescription')
            paramlist = root.find('paramlist')
            #for parameter in paramlist:
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

            if meta['Ion Source'] == 'MALDI Imaging':
                # Need to collect the images here
                pass
            else:
                datefield = None
                grandparent_dir = os.path.dirname(os.path.dirname(m_dir))
                parent_dirs = get_immediate_subdirectories(grandparent_dir)
                grandparent_dirname = os.path.split(grandparent_dir)[-1]
                if grandparent_dirname.isdigit(): # Then it is a date field so step up further
                    datefield = grandparent_dir
                    grandparent_dir = os.path.dirname(grandparent_dir)
                print(grandparent_dirname.isdigit())
                print(grandparent_dir)
                print(parent_dirs)
            print(meta)
                         
