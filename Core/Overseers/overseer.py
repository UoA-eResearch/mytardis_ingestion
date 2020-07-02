# overseer.py
#
# Abstract class definition for the MyTardis overseer class.
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 02 Jul 2020
#

from abc import ABC, abstractmethod
from ..helpers import MyTardisRESTFactory
from ..helpers import RAiDFactory
from urllib.parse import urlparse

# Import minions
from project_minion import ProjectMinion
from experiment_minion import ExperimentMinion
from dataset_minion import DatasetMinion
from datafile_minion import DatafileMinion
from schema_minions import SchemaMinion

from ..helpers import UnableToFindUniqueError
from ..helpers import SanityCheckError

import os
import logging

logger = logging.getLogger(__name__)


class MyTardisOverseer():
    '''
    Abstract observer class:
    Overseer classes inspect the MyTardis database to ensure
    that the forge is not creating existing objects and validates
    that the heirarchical structures needed are in place before
    a new object is created.
    '''

    def __init__(self,
                 global_config_filepath,
                 local_config_filepath):
        self.rest_factory = MyTardisRESTFactory(local_config_filepath)
        self.raid_factory = RAiDFactory(global_config_filepath)
        self.project_minion = ProjectMinion(global_config_filepath,
                                            local_config_filepath)
        self.experiment_minion = ExperimentMinion(global_config_filepath,
                                                  local_config_filepath)
        self.dataset_minion = DatasetMinion(global_config_filepath,
                                            local_config_filepath)
        self.datafile_minion = DatafileMinion(global_config_filepath,
                                              local_config_filepath)
        self.schema_minion = SchemaMinion(global_config_filepath,
                                          local_config_filepath)

    def __resource_uri_to_id(self, uri):
        """
        Takes resource URI like: http://example.org/api/v1/experiment/998
        and returns just the id value (998).
        #
        ======================
        Inputs
        ======================
        uri: str - the URI from MyTardis
        ======================
        Returns
        ======================
        resource_id: int - the integer id that maps to the URI
        """
        resource_id = int(urlparse(uri).path.rstrip(
            os.sep).split(os.sep).pop())
        return resource_id

    # Check to see if a project with the associated RAiD exists
    # If it does, validate the values in the project dictionary
    # If these vary log a warning and update the dictionary with the
    # existing values from within MyTardis
    def validate_project(self,
                         project_dict):
        # First validate project dictionary using the minion
        try:
            valid = self.project_minion.validate_dictionary(project_dict)
        except SanityCheckError as error:
            logger.warning(f'Project {project_dict["project"]} failed sanity' +
                           f' check. Missing keys: {error.missing_keys}')
            return None
        except Exception as error:
            logger.error(error)
            raise error
        # Then check the dictionaires are 'similar'
        if valid:
            try:
                uri, obj = self.project_minion.get_from_raid(
                    project_dict['raid'])
            except UnableToFindUniqueError as error:
                # We should never get here since DB enforces uniqueness
                # If we are here something really wrong has happened
                logger.critical(f'Multiple projects with the same RAiD: ' +
                                f'{project_dict["raid"]}, found')
                raise error
            except Exception as error:
                logger.error(error)
                raise error
            if uri:
                if project_dict['name'] != obj['name']:
                    logger.warning(f'Project names differ between project ' +
                                   f'dictionary: {project_dict["name"]}, ' +
                                   f'and object in MyTardis: {obj["name"]}')
                    project_dict['name'] = obj['name']
                if project_dict['description'] != obj['description']:
                    logger.warning(f'Project descriptions differ between project ' +
                                   f'dictionary: {project_dict["description"]}, ' +
                                   f'and object in MyTardis: {obj["description"]}')
                    project_dict['description'] = obj['description']
                if project_dict['lead_researcher'] != obj['lead_researcher']:
                    logger.warning(f'Project Lead differs between project ' +
                                   f'dictionary: {project_dict["lead_researcher"]}, ' +
                                   f'and object in MyTardis: {obj["lead_researcher"]}')
                    project_dict['leader_researcher'] = obj['leader_researcher']
        return project_dict

    def validate_experiment(self,
                            experiment_dict):
        # First validate experiment dictionary using the minion
        try:
            valid = self.experiment_minion.validate_dictionary(experiment_dict)
        except SanityCheckError as error:
            logger.warning(f'Experiment {experiment_dict["title"]} failed ' +
                           f'sanity check. Missing keys: {error.missing_keys}')
            return None
        except Exception as error:
            logger.error(error)
            raise error
        if valid:
            try:
                uri, obj = self.experiment_minion.get_from_raid(
                    experiment_dict['raid'])
            except UnableToFindUniqueError as error:
                # We should never get here since DB enforces uniqueness
                # If we are here something really wrong has happened
                logger.critical(f'Multiple experiments with the same RAiD: ' +
                                f'{experiment_dict["raid"]}, found')
                raise error
            except Exception as error:
                logger.error(error)
                raise error
            if uri:
                if experiment_dict['title'] != obj['title']:
                    logger.warning(f'Experiment names differ between experiment ' +
                                   f'dictionary: {experiment_dict["title"]}, ' +
                                   f'and object in MyTardis: {obj["title"]}')
                    experiment_dict['title'] = obj['title']
                if experiment_dict['description'] != obj['description']:
                    logger.warning(f'Experiment names differ between experiment ' +
                                   f'dictionary: {experiment_dict["description"]}, ' +
                                   f'and object in MyTardis: {obj["description"]}')
                    experiment_dict['description'] = obj['description']
        return experiment_dict
