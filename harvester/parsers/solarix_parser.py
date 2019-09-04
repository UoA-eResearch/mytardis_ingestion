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

class SolarixParser(DirParser):

    def __init__(self,
                 config_dict):
        super().__init__(config_dict)

    def create_datafile_dicts(self):
        pass

    def create_dataset_dicts(self):
        pass

    def create_experiment_dicts(self):
        pass

    def walk_directory(self):
        '''Use os.walk to get the subdirectories under the root directory
        and use these to locate data and metadata files'''

        m_dirs = []
        for path, directory, filename in os.walk(self.root_dir):
            if 'apexAcquisition.method' in filename:
                m_dirs.append(path)
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
            tree = ElementTree.parse(os.path.join(m_dir,'apexAcquisition.method'))
            root = tree.getroot()
            paramlist = root.find('paramlist')
            #for parameter in paramlist:
            params = paramlist.iterfind('param')
            for param in params:
                if param.attrib['name'] in key_list:
                    value = param.find('value')    
                    param_dict[param.attrib['name']] = value.text
            meta = {}
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
                
            print(meta)
                         
