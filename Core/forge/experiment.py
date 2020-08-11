# project.py
#
# Class to ingest experiment data into MyTardis
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 03 Aug 2020
#

from ..overseer import Overseer
from .. import MyTardisRESTFactory
from ..helpers import dict_to_json
import json
import logging

logger = logging.getLogger(__name__)


class ExperimentForge():

    def __init__(self,
                 local_config_file_path):
        self.overseer = Overseer(local_config_file_path)
        self.rest_factory = MyTardisRESTFactory(local_config_file_path)

    def get_or_create(self,
                      input_dict):
        try:
            input_dict, schema, uri, obj = self.overseer.validate_experiment(
                input_dict)
        except Exception as error:
            raise error
        if uri:
            experiment_id = obj['id']
            return (uri, experiment_id)
        else:
            mytardis, parameters = self.smelt(input_dict,
                                              schema)
        if not mytardis:
            return (None, None)
        else:
            uri, experiment_id = self.forge(mytardis,
                                            parameters)
            return (uri, experiment_id)

    def smelt(self,
              input_dict,
              schema):
        base_keys = {'title',
                     'raid',
                     'description',
                     'project',
                     'start_time',
                     'end_time',
                     'created_time',
                     'update_time',
                     'embargo_until',
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
            response = self.rest_factory.post_request('experiment',
                                                      mytardis_json)
        except Exception as error:
            logger.error(f'Unable to create experiment: {mytardis["title"]}. ' +
                         f'Error returned: {error}')
            raise error
        body = json.loads(response.text)
        uri = body['resource_uri']
        experiment_id = body['id']
        if parameters:
            parameters['experiment'] = uri
            parameters_json = dict_to_json(parameters)
            try:
                response = self.rest_factory.post_request('experimentparameterset',
                                                          parameters_json)
            except Exception as error:
                logger.warning(f'Unable to attach metadata to project: {mytardis["title"]}. ' +
                               f' Error returned: {error}')
        return (uri, experiment_id)
