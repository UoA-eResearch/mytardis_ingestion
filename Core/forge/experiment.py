# experiment.py
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
                           f'Not building experiment: {input_dict["title"]}')
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
                logger.warning(f'Unable to attach metadata to experiment: {mytardis["title"]}. ' +
                               f' Error returned: {error}')
        return (uri, experiment_id)

    def reforge(self,
                input_dict):
        # Check for the experiment in the database
        # If it is there then reforge, otherwise forge.

        try:
            input_dict, schema, uri, obj = self.overseer.validate_experiment(
                input_dict,
                overwrite=True)
            print('Schema')
            print(schema)
        except Exception as error:
            raise error
        if uri:
            obj_id = obj['id']
            response = self.rest_factory.get_request('experiment',
                                                     None,
                                                     obj_id=obj_id)
            resp = json.loads(response.text)
        print(resp)
        mytardis, parameters = self.smelt(input_dict,
                                          schema)
        print(mytardis)
        print(parameters)
        if uri:
            for key in mytardis.keys():
                resp[key] = mytardis[key]
            print(resp)
            mytardis_json = dict_to_json(mytardis)
            print(mytardis_json)
        else:
            mytardis_json = dict_to_json(mytardis)
        if uri:
            experiment_id = obj['id']
            try:
                print('putting')
                print(mytardis_json)
                response = self.rest_factory.put_request('experiment',
                                                         mytardis_json,
                                                         experiment_id)
            except Exception as error:
                logger.error(f'Unable to update experiment: {mytardis["title"]}. ' +
                             f'Error returned: {error}')
                raise error
            if parameters:
                print(parameters)
                reforge_flg = True
                try:
                    # only one param set here
                    parameters_id = obj['parameter_sets'][0]['id']
                except KeyError:
                    reforge_flg = False
                except Exception as error:
                    raise error
                parameters['experiment'] = uri
                parameters_json = dict_to_json(parameters)
                if reforge_flg:
                    try:
                        print(parameters_id)
                        print(parameters_json)
                        response = self.rest_factory.put_request('experimentparameterset',
                                                                 parameters_json,
                                                                 parameters_id)
                    except Exception as error:
                        logger.warning(f'Unable to update metadata to experiment: {mytardis["title"]}. ' +
                                       f' Error returned: {error}')
                    return (uri, experiment_id)
                else:
                    try:
                        response = self.rest_factory.post_request('experimentparameterset',
                                                                  parameters_json)
                    except Exception as error:
                        logger.warning(f'Unable to attach metadata to experiment: {mytardis["title"]}. ' +
                                       f' Error returned: {error}')
                    return (uri, experiment_id)
        else:
            uri, experiment_id = self.forge(mytardis,
                                            parameters)
            return (uri, experiment_id)
