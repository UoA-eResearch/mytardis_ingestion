# overseer.py
#
# Abstract class definition for the MyTardis overseer class.
#
# written by Chris Seal <c.seal@auckland.ac.nz>
#
# Last updated: 05 Aug 2020
#

from urllib.parse import urlparse

# Import minions
from .project_minion import ProjectMinion
from .experiment_minion import ExperimentMinion
from .dataset_minion import DatasetMinion
from .datafile_minion import DatafileMinion
from .schema_minion import SchemaMinion
from .institution_minion import InstitutionMinion
from .instrument_minion import InstrumentMinion

from ..helpers import UnableToFindUniqueError
from ..helpers import SanityCheckError
from ..helpers import HierarchyError
from ..helpers import readJSON, writeJSON
from ..helpers import calculate_md5sum, calculate_etag

import os
import logging

logger = logging.getLogger(__name__)

KB = 1024
MB = KB ** 2
GB = KB ** 3


class Overseer():
    '''
    Observer class:
    Overseer classes inspect the MyTardis database to ensure
    that the forge is not creating existing objects and validates
    that the heirarchical structures needed are in place before
    a new object is created.
    '''

    def __init__(self,
                 local_config_file_path):
        self.local_config_file_path = local_config_file_path

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
                         project_dict,
                         overwrite=False):
        project_minion = ProjectMinion(self.local_config_file_path)
        # First validate project dictionary using the minion
        try:
            valid = project_minion.validate_dictionary(project_dict)
        except SanityCheckError as error:
            logger.warning(f'Project {project_dict["name"]} failed sanity' +
                           f' check. Missing keys: {error.missing_keys}')
            return None
        except Exception as error:
            logger.error(error)
            raise error
        # Then check the dictionaires are 'similar'
        if valid:
            try:
                uri, obj = project_minion.get_from_raid(
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
            if uri and not overwrite:
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
                # Deactivate user check since upis are passed and uris returned
                '''if project_dict['lead_researcher'] != obj['lead_researcher']:
                    logger.warning(f'Project Lead differs between project ' +
                                   f'dictionary: {project_dict["lead_researcher"]}, ' +
                                   f'and object in MyTardis: {obj["lead_researcher"]}')
                    project_dict['lead_researcher'] = obj['lead_researcher']'''
        try:
            schema_valid = self.validate_schema(project_dict,
                                                11)
        except Exception as error:
            raise error
        return (project_dict, schema_valid, uri, obj)

    def validate_experiment(self,
                            experiment_dict,
                            overwrite=False):
        project_minion = ProjectMinion(self.local_config_file_path)
        experiment_minion = ExperimentMinion(self.local_config_file_path)
        # First validate experiment dictionary using the minion
        try:
            valid = experiment_minion.validate_dictionary(experiment_dict)
        except SanityCheckError as error:
            logger.warning(f'Experiment {experiment_dict["title"]} failed ' +
                           f'sanity check. Missing keys: {error.missing_keys}')
            return None
        except Exception as error:
            logger.error(error)
            raise error
        if valid:
            try:
                uri, _ = project_minion.get_from_raid(
                    experiment_dict['project'])
            except Exception as error:
                logger.error(error)
                raise error
            else:
                if not uri:
                    logger.warning(f'Unable to create experiment {experiment_dict["title"]}' +
                                   f' as the project RAiD {experiment_dict["project"]} ' +
                                   f'could not be found in the database')
                    raise HierarchyError(experiment_dict["project"],
                                         experiment_dict['title'])
                else:
                    experiment_dict['project'] = uri
            try:
                uri, obj = experiment_minion.get_from_raid(
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
            if uri and not overwrite:
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
        try:
            schema_valid = self.validate_schema(experiment_dict,
                                                1)
        except Exception as error:
            raise error
        return (experiment_dict, schema_valid, uri, obj)

    def validate_dataset(self,
                         dataset_dict,
                         overwrite=False):
        experiment_minion = ExperimentMinion(self.local_config_file_path)
        dataset_minion = DatasetMinion(self.local_config_file_path)
        instrument_minion = InstrumentMinion(self.local_config_file_path)
        # First validate dataset dictionary using the minion
        try:
            valid = dataset_minion.validate_dictionary(dataset_dict)
        except SanityCheckError as error:
            logger.warning(f'Dataset {dataset_dict["description"]} failed ' +
                           f'sanity check. Missing keys: {error.missing_keys}')
            return None
        except Exception as error:
            logger.error(error)
            raise error
        if valid:
            try:
                uri, _ = experiment_minion.get_from_raid(
                    dataset_dict['experiments'][0])
            except Exception as error:
                logger.error(error)
                raise error
            else:
                if not uri:
                    logger.warning(f'Unable to create dataset {dataset_dict["description"]}' +
                                   f' as the experiment RAiD {dataset_dict["experiments"]} ' +
                                   f'could not be found in the database')
                    raise HierarchyError(dataset_dict["experiments"],
                                         dataset_dict['description'])
                else:
                    dataset_dict['experiments'] = [uri]
            try:
                uri, obj = instrument_minion.get_from_instrument_id(
                    dataset_dict['instrument_id'])
            except UnableToFindUniqueError as error:
                # We should never get here since DB enforces uniqueness
                # If we are here something really wrong has happened
                logger.critical(f'Multiple datasets with the same RAiD: ' +
                                f'{dataset_dict["dataset_id"]}, found')
                raise error
            except Exception as error:
                logger.error(error)
                raise error
            if not uri:
                logger.warning(f'No instrument with id {dataset_dict["instrument_id"]} ' +
                               f'found. Unable to create dataset')
                return None
            else:
                dataset_dict['instrument'] = uri
                dataset_dict.pop('instrument_id')
            try:
                uri, obj = dataset_minion.get_from_raid(
                    dataset_dict['dataset_id'])
            except UnableToFindUniqueError as error:
                # We should never get here since DB enforces uniqueness
                # If we are here something really wrong has happened
                logger.critical(f'Multiple datasets with the same RAiD: ' +
                                f'{dataset_dict["dataset_id"]}, found')
                raise error
            except Exception as error:
                logger.error(error)
                raise error
            if uri and not overwrite:
                if dataset_dict['description'] != obj['description']:
                    logger.warning(f'Dataset names differ between dataset ' +
                                   f'dictionary: {dataset_dict["description"]}, ' +
                                   f'and object in MyTardis: {obj["description"]}')
                    dataset_dict['description'] = obj['description']
        try:
            schema_valid = self.validate_schema(dataset_dict,
                                                2)
        except Exception as error:
            raise error
        return (dataset_dict, schema_valid, uri, obj)

    def validate_datafile(self,
                          datafile_dict,
                          overwrite=True):
        dataset_minion = DatasetMinion(self.local_config_file_path)
        datafile_minion = DatafileMinion(self.local_config_file_path)
        # First validate the datafile dictionary usin the minion
        try:
            valid = datafile_minion.validate_dictionary(datafile_dict)
        except SanityCheckError as error:
            logger.warning(f'Datafile {datafile_dict["filename"]} failed ' +
                           f'sanity check. Missing keys: {error.missing_keys}')
            return None
        except Exception as error:
            logger.error(error)
            raise error
        if valid:
            try:
                uri, _ = dataset_minion.get_from_raid(
                    datafile_dict['dataset_id'])
            except Exception as error:
                logger.error(error)
                raise error
            else:
                if not uri:
                    logger.warning(f'Unable to create dataset {datafile_dict["filename"]}' +
                                   f' as the experiment RAiD {datafile_dict["dataset_id"]} ' +
                                   f'could not be found in the database')
                    raise HierarchyError(datafile_dict["dataset_id"],
                                         datafile_dict['filename'])
                else:
                    datafile_dict['dataset'] = uri
                    uri, obj = datafile_minion.check_file_exists(
                        datafile_dict)
                    if uri and not overwrite:
                        logger.warning(f'File {datafile_dict["filemane"]} already' +
                                       f'exists in MyTardis, skipping')
                    datafile_dict.pop('dataset_id')
        try:
            schema_valid = self.validate_schema(datafile_dict,
                                                3)
        except Exception as error:
            raise error
        return (datafile_dict, schema_valid, uri, obj)

    def build_checksum_digest(checksum_digest,
                              root_dir,
                              file_path,
                              s3=True,
                              s3_blocksize=1*GB,
                              md5_blocksize=None,
                              subprocess_size_threshold=10*MB,
                              md5sum_executable='/usr/bin/md5sum'):
        """
        Builds a tuple of md5, etag checksums for a given
        data file and appends it to the checksum digest file for use
        by the ingestion classes.
        #
        =================================
        Inputs:
        =================================
        checksum_digest: a Path object to the file to append to
        file_path: a Path object to the file to build the checksums force
        s3: a boolean flag to indicate whether or not to calculate the ETag
        #
        =================================
        Returns:
        =================================
        True: if checksums calculated and appended successfully
        False: otherwise
        """
        abs_file_path = root_dir / file_path
        checksum_dict = {}
        if not os.path.isfile(checksum_digest):
            checksum_dict = {}
        else:
            checksum_dict = readJSON(checksum_digest)
        if not file_path in checksum_dict.keys():
            checksum_dict[file_path.as_posix()] = {}
        checksum_dict[file_path.as_posix()]['md5sum'] = calculate_md5sum(
            abs_file_path)
        if s3:
            checksum_dict[file_path.as_posix()]['etag'] = calculate_etag(abs_file_path,
                                                                         s3_blocksize)
        writeJSON(checksum_dict, checksum_digest)
        return checksum_dict

    def validate_schema(self,
                        input_dict,
                        model_int):
        schema_minion = SchemaMinion(self.local_config_file_path)
        '''SCHEMA TYPES -> These are the model_ints:
        EXPERIMENT = 1
        DATASET = 2
        DATAFILE = 3
        NONE = 4
        INSTRUMENT = 5
        PROJECT = 11'''
        if 'schema' not in input_dict.keys():
            logger.warning(f'No schema defined in the input dictionary')
            return None
        try:
            uri, obj = schema_minion.get_from_namespace(
                input_dict['schema'])
        except Exception as error:
            logger.error(error)
            raise error
        schema_type = obj['schema_type']
        if schema_type != model_int:
            return False
        else:
            return uri

    def validate_institution(self,
                             ror):
        institution_minion = InstitutionMinion(self.local_config_file_path)
        try:
            uri, obj = institution_minion.get_from_ror(ror)
        except Exception as error:
            logger.warning(f'Institution (ROR: {ror}) unable to be found in ' +
                           f'the database')
            logger.warning(error)
            raise error
        if uri:
            return (uri, obj)
        else:
            return (None, None)

    def validate_instrument(self,
                            instrument_id):
        institution_minion = InstrumentMinion(self.global_config_filepath,
                                              self.local_config_filepath)
        try:
            uri, obj = institution_minion.get_from_ror(ror)
        except Exception as error:
            logger.warning(f'Instrument (ID: {instrument_id}) unable to be found in ' +
                           f'the database')
            logger.warning(error)
            raise error
        if uri:
            return (uri, obj)
        else:
            return (None, None)
