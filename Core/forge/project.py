# project.py
#
# Class to ingest project data into MyTardis
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 03 Aug 2020
#

from ..overseer import Overseer
from .. import MyTardisRESTFactory
from ..helpers import dict_to_json
import logging
import json

logger = logging.getLogger(__name__)

UOA_ROR = 'https://ror.org/03b94tp07'


class ProjectForge():

    def __init__(self,
                 local_config_file_path):
        self.overseer = Overseer(local_config_file_path)
        self.rest_factory = MyTardisRESTFactory(local_config_file_path)

    def get_or_create(self,
                      input_dict):
        try:
            input_dict, schema, uri, obj = self.overseer.validate_project(
                input_dict)
        except Exception as error:
            raise error
        if uri:
            project_id = obj['id']
            return (uri, project_id)
        else:
            mytardis, parameters = self.smelt(input_dict,
                                              schema)
        if not mytardis:
            return (None, None)
        else:
            uri, project_id = self.forge(mytardis,
                                         parameters)
            return (uri, project_id)

    def smelt(self,
              input_dict,
              schema):
        base_keys = {'name',
                     'raid',
                     'description',
                     'url',
                     'start_date',
                     'end_date',
                     'embargo_until',
                     'lead_researcher',
                     'admins',
                     'admin_groups',
                     'members',
                     'member_groups'}
        mytardis = {}
        parameters = {}
        # TODO: Refactor to account for multiple institutions
        if 'institution' in input_dict.keys():
            ror = input_dict['institution']
            input_dict.pop('institution')
        else:
            ror = UOA_ROR
        institution, _ = self.overseer.validate_institution(ror)
        if not institution:
            logger.error(f'Unable to create project {input_dict["name"]}. ' +
                         f'Incorrect ROR identifier given')
            return (None, None)
        else:
            mytardis['institution'] = [institution]
        if not schema:
            logger.warning(f'Schema {input_dict["schema"]} not found in database.' +
                           f'Not building project: {input_dict["name"]}')
            return (None, None)
        else:
            input_dict.pop('schema')
            parameters['schema'] = schema
        if 'admins' in input_dict.keys():
            input_dict['admins'] = list(set(input_dict['admins']))
            if 'lead_researcher' in input_dict.keys():
                if input_dict['lead_researcher'] in input_dict['admins']:
                    input_dict['admins'].pop(input_dict['lead_researcher'])
        for key in input_dict.keys():
            if key in base_keys:
                mytardis[key] = input_dict[key]
            else:
                if 'parameters' not in parameters.keys():
                    parameters['parameters'] = []
                parameters['parameters'].append({u'name': key,
                                                 u'value': input_dict[key]})
        if 'parameters' in parameters.keys() and parameters['parameters'] == []:
            parameters = None
        return (mytardis, parameters)

    def forge(self,
              mytardis,
              parameters):
        mytardis_json = dict_to_json(mytardis)
        try:
            response = self.rest_factory.post_request('project',
                                                      mytardis_json)
        except Exception as error:
            logger.error(f'Unable to create project: {mytardis["name"]}. ' +
                         f'Error returned: {error}')
            raise error
        body = json.loads(response.text)
        uri = body['resource_uri']
        project_id = body['id']
        if parameters:
            parameters['project'] = uri
            parameters_json = dict_to_json(parameters)
            try:
                response = self.rest_factory.post_request('projectparameterset',
                                                          parameters_json)
            except Exception as error:
                logger.warning(f'Unable to attach metadata to project: {mytardis["name"]}. ' +
                               f' Error returned: {error}')
        return (uri, project_id)

    def reforge(self,
                input_dict):
        # Check for the project in the database
        # If it is there then reforge, otherwise forge.
        try:
            input_dict, schema, uri, obj = self.overseer.validate_project(
                input_dict,
                overwrite=True)
        except Exception as error:
            raise error
        mytardis, parameters = self.smelt(input_dict,
                                          schema)
        mytardis_json = dict_to_json(mytardis)
        if uri:
            project_id = obj['id']
            try:
                response = self.rest_factory.put_request('project',
                                                         mytardis_json,
                                                         project_id)
            except Exception as error:
                logger.error(f'Unable to update project: {mytardis["name"]}. ' +
                             f'Error returned: {error}')
                raise error
            if parameters:
                reforge_flg = True
                try:
                    # only one param set here
                    parameters_id = obj['parameter_sets'][0]['id']
                except KeyError:
                    reforge_flg = False
                except Exception as error:
                    raise error
                parameters['project'] = uri
                parameters_json = dict_to_json(parameters)
                if reforge_flg:
                    try:
                        response = self.rest_factory.put_request('projectparameterset',
                                                                 parameters_json,
                                                                 parameters_id)
                    except Exception as error:
                        logger.warning(f'Unable to update metadata to project: {mytardis["name"]}. ' +
                                       f' Error returned: {error}')
                    return (uri, project_id)
                else:
                    try:
                        response = self.rest_factory.post_request('projectparameterset',
                                                                  parameters_json)
                    except Exception as error:
                        logger.warning(f'Unable to attach metadata to project: {mytardis["name"]}. ' +
                                       f' Error returned: {error}')
                    return (uri, project_id)
        else:
            uri, project_id = self.forge(mytardis,
                                         parameters)
            return (uri, project_id)
