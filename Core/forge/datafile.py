# datafile.py
#
# Class to ingest data files into MyTardis
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 23 Jul 2020
#

from ..overseer import Overseer
from .. import MyTardisRESTFactory
from ..helpers import dict_to_json
import logging
import json

logger = logging.getLogger(__name__)


class DatafileForge():

    def __init__(self,
                 local_config_file_path):
        self.overseer = Overseer(local_config_file_path)
        self.rest_factory = MyTardisRESTFactory(local_config_file_path)

    def get_or_create(self,
                      input_dict):
        try:
            input_dict, schema, uri, obj = self.overseer.validate_datafile(
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
            uri, datafile_id = self.forge(mytardis,
                                          parameters)
            return (uri, datafile_id)

    def smelt(self,
              input_dict,
              schema):
        base_keys = {'dataset',
                     'filename',
                     'md5sum',
                     'directory',
                     'mimetype',
                     'size',
                     'lead_researcher',
                     'admins',
                     'admin_groups',
                     'members',
                     'member_groups'}
        mytardis = {}
        parameters = {}
        if not schema:
            logger.warning(f'Schema {input_dict["schema"]} not found in database.' +
                           f'Not building datafile: {input_dict["name"]}')
            return (None, None)
        else:
            input_dict.pop('schema')
            parameters['schema'] = schema
        replica, input_dict = self.create_replica(input_dict)
        mytardis['replicas'] = [replica]
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

    def create_replica(self,
                       input_dict):
        uri = input_dict.pop('full_path')
        location = input_dict.pop('storage_box')
        replica = {'uri': uri,
                   'location': location,
                   'protocol': 'file'}
        return (replica, input_dict)

    def forge(self,
              mytardis,
              parameters):
        mytardis_json = dict_to_json(mytardis)
        try:
            response = self.rest_factory.post_request('dataset_file',
                                                      mytardis_json)
        except Exception as error:
            logger.error(f'Unable to create datafile: {mytardis["filename"]}. ' +
                         f'Error returned: {error}')
            raise error
        body = json.loads(response.text)
        uri = body['resource_uri']
        datafile_id = body['id']
        if parameters:
            parameters['datafile'] = uri
            parameters_json = dict_to_json(parameters)
            try:
                response = self.rest_factory.post_request('datafileparameterset',
                                                          parameters_json)
            except Exception as error:
                logger.warning(f'Unable to attach metadata to project: {mytardis["filename"]}. ' +
                               f' Error returned: {error}')
        return (uri, datafile_id)

    def reforge(self,
                input_dict):
        # Check for the experiment in the database
        # If it is there then reforge, otherwise forge.
        try:
            input_dict, schema, uri, obj = self.overseer.validate_datafile(
                input_dict,
                overwrite=True)
        except Exception as error:
            raise error
        mytardis, parameters = self.smelt(input_dict,
                                          schema)
        mytardis_json = dict_to_json(mytardis)
        if uri:
            datafile_id = obj['id']
            try:
                response = self.rest_factory.put_request('dataset_file',
                                                         mytardis_json,
                                                         datafile_id)
            except Exception as error:
                logger.error(f'Unable to update datafile: {mytardis["filename"]}. ' +
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
                parameters['datafile'] = uri
                parameters_json = dict_to_json(parameters)
                if reforge_flg:
                    try:
                        response = self.rest_factory.put_request('datafileparameterset',
                                                                 parameters_json,
                                                                 parameters_id)
                    except Exception as error:
                        logger.warning(f'Unable to update metadata to datafile: {mytardis["filename"]}. ' +
                                       f' Error returned: {error}')
                    return (uri, datafile_id)
                else:
                    try:
                        response = self.rest_factory.post_request('datafileparameterset',
                                                                  parameters_json)
                    except Exception as error:
                        logger.warning(f'Unable to attach metadata to datafile: {mytardis["filename"]}. ' +
                                       f' Error returned: {error}')
                    return (uri, datafile_id)
        else:
            uri, datafile_id = self.forge(mytardis,
                                          parameters)
            return (uri, datafile_id)
