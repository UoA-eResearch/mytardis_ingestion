# dataset.py
#
# Class to ingest dataset data into MyTardis
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 05 Aug 2020
#

from ..overseer import Overseer
from .. import MyTardisRESTFactory
from ..helpers import dict_to_json
import json
import logging

logger = logging.getLogger(__name__)


class DatasetForge():

    def __init__(self,
                 local_config_file_path):
        self.overseer = Overseer(local_config_file_path)
        self.rest_factory = MyTardisRESTFactory(local_config_file_path)

    def get_or_create(self,
                      input_dict):
        try:
            input_dict, schema, uri, obj = self.overseer.validate_dataset(
                input_dict)
        except Exception as error:
            raise error
        if uri:
            dataset_id = obj['id']
            return (uri, dataset_id)
        else:
            mytardis, parameters = self.smelt(input_dict,
                                              schema)
        if not mytardis:
            return (None, None)
        else:
            uri, dataset_id = self.forge(mytardis,
                                         parameters)
            return (uri, dataset_id)

    def smelt(self,
              input_dict,
              schema):
        base_keys = {'experiments',
                     'dataset_id',
                     'description',
                     'directory',
                     'created_time',
                     'modified_time',
                     'instrument',
                     'lead_researcher',
                     'admins',
                     'admin_groups',
                     'members',
                     'member_groups'}
        mytardis = {}
        parameters = {}
        if not schema:
            logger.warning(f'Schema {input_dict["schema"]} not found in database.' +
                           f'Not building project: {input_dict["name"]}')
            return (None, None)
        else:
            input_dict.pop('schema')
            parameters['schema'] = schema
        for key in input_dict.keys():
            if key in base_keys:
                mytardis[key] = input_dict[key]
            else:
                if 'parameters' not in parameters.keys():
                    parameters['parameters'] = []
                parameters['parameters'].append({u'name': key,
                                                 u'value': input_dict[key]})
        if parameters['parameters'] == []:
            parameters = None
        return (mytardis, parameters)

    def forge(self,
              mytardis,
              parameters):
        mytardis_json = dict_to_json(mytardis)
        try:
            response = self.rest_factory.post_request('dataset',
                                                      mytardis_json)
        except Exception as error:
            logger.error(f'Unable to create dataset: {mytardis["description"]}. ' +
                         f'Error returned: {error}')
            raise error
        body = json.loads(response.text)
        uri = body['resource_uri']
        dataset_id = body['id']
        if parameters:
            parameters['dataset'] = uri
            parameters_json = dict_to_json(parameters)
            try:
                response = self.rest_factory.post_request('datasetparameterset',
                                                          parameters_json)
            except Exception as error:
                logger.warning(f'Unable to attach metadata to dataset: {mytardis["description"]}. ' +
                               f' Error returned: {error}')
        return (uri, dataset_id)
