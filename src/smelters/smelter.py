# pylint: disable=logging-fstring-interpolation
"""Smelter base class. A class that provides functions to split input dictionaries into
dictionaries suited to be passed into an instance of the Forge class for creating
objects in MyTardis."""
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import List, Optional, Tuple

from pydantic import ValidationError
from src.helpers import SanityCheckError, calculate_md5sum, sanity_check
from src.helpers.config import (
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
)

from src.blueprints import (
    BaseObjectType,
    DatafileReplica,
    ParameterSet,
    RawDatafile,
    RawDataset,
    RawExperiment,
    RawProject,
)
from src.helpers import (
    DATAFILE_KEYS,
    DATASET_KEYS,
    EXPERIMENT_KEYS,
    PROJECT_KEYS,
    project_enabled,
)
from src.helpers.exceptions import SanityCheckError

logger = logging.getLogger(__name__)


class Smelter(ABC):
    """The Smelter base class to be subclassed into individual concrete classes for different
    ingestion approaches.

    Smelter classes share a number of similar processing routines, especially around dataclass
    modification and date handling.

    Attributes:

    """

    def __init__(
        self,
        general: GeneralConfig,
        default_schema: SchemaConfig,
        storage: StorageConfig,
        mytardis_setup: IntrospectionConfig = None,
    ) -> None:
        """Class initialisation to set options for dictionary processing

        Stores MyTardis set up information from the introspection API to allow the parser
        to prepare only objects that exist and to handle additional keys in the dictionaries
        gracefully.

        Args:
            projects_enabled: a boolean flag indicating whether or not to process projects
            objects_with_ids: a list of objects that have identifiers. Defaults to empty
            objects_with_profiles: a list of objects that have profiles. Defaults to empty
            default_schema: a dictionary of schema namespaces to use for projects,
                experiments, datasets and datafiles
            source_dir: a Path object leading to the relative root directory containing
                the files to be ingested
            target_dir: a Path object leading to the relatvie root directory containing
                the files when they have been ingested into MyTardis
            storage_box: a StorageBox object containing the location of the root
                directory of the storage box.
        """
        # NOTE: This is out of date and should be updated to use the Pydantic Config
        # prepared by Lukas

        self.projects_enabled = mytardis_config["projects_enabled"]
        try:
            self.objects_with_ids = mytardis_config["objects_with_ids"]
        except KeyError:
            self.objects_with_ids = []
        try:
            self.objects_with_profiles = mytardis_config["objects_with_profiles"]
        except KeyError:
            self.objects_with_profiles = []
        try:
            self.default_schema = mytardis_config["default_schema"]
        except KeyError:
            self.default_schema = None
        try:
            self.default_institution = mytardis_config["default_institution"]
        except KeyError:
            self.default_institution = None
        self.source_dir = Path(mytardis_config["source_directory"])
        self.target_dir = Path(mytardis_config["target_directory"])
        self.storage_box = mytardis_config["storage_box"]
        self.use_project = project_enabled

    def precondition_input_dictionary(self, parsed_dict) -> dict:
        """Function to precondition the raw input from the file read. This ensures
        that any key translation is carried out as well as tidying up the metadata
        keys and user stuff

        Args:
            parsed_dict: raw dictionary as parsed from its input file

        Returns:
            A tuple containing the object type and the cleaned dictionary
        """
        object_type, cleaned_dict = self.tidy_up_dictionary_keys(parsed_dict)
        cleaned_dict = self._tidy_up_metadata_keys(cleaned_dict, object_type)
        users, groups = Smelter.parse_groups_and_users_from_separate_access(
            cleaned_dict
        )
        if users != []:
            cleaned_dict["users"] = users
        if groups != []:
            cleaned_dict["groups"] = groups
        return cleaned_dict

    def smelt_project(self, cleaned_dict: dict) -> tuple:
        """Wrapper to check and create the python dictionaries in a form expected by the
        forge class.

        Args:
            cleaned_dict: dictionary containing the object data and metadata

        Returns:
            A tuple containing an object dictionary, which is the minimum metadata required
            to create the object in MyTardis, and a parameter dictionary containing the
            additional metadata.
        """
        if not self.projects_enabled:
            logger.warning(
                (
                    "MyTardis is not currently set up to use projects. Please check settings.py "
                    "and ensure that the 'projects' app is enabled. This may require rerunning "
                    "migrations."
                )
            )
            return (None, None)
        cleaned_dict = self.precondition_input_dictionary(cleaned_dict)
        object_keys = [
            "name",
            "description",
            "locked",
            "public_access",
            "principal_investigator",
            "institution",
            "embargo_until",
            "start_time",
            "end_time",
            "created_by",
            "url",
            "users",
            "groups",
        ]
        if self.objects_with_ids and "project" in self.objects_with_ids:
            object_keys.append("persistent_id")
            object_keys.append("alternate_ids")
        if "schema" not in cleaned_dict.keys():
            try:
                cleaned_dict = self._inject_schema_from_default_value(
                    cleaned_dict["name"], "project", cleaned_dict
                )
            except SanityCheckError:
                logger.warning(
                    "Unable to find default project schema and no schema provided"
                )
                return (None, None)
        if "institution" not in cleaned_dict.keys():
            if self.default_institution:
                cleaned_dict["institution"] = self.default_institution
            else:
                logger.warning(
                    "Unable to find default institution and no institution provided"
                )
                return (None, None)
        if not self._verify_project(cleaned_dict):
            return (None, None)
        return self._smelt_object(object_keys, cleaned_dict)

    def _inject_schema_from_default_value(self, name, object_type, cleaned_dict):
        """Generic function to inject the schema into the cleaned dictionary provided
        one exists.

        Args:
            name: the object name
            object_type: the object type defined by the dictionary
            cleaned_dict: The dictionary to inject schema into

        Returns:
            the dictionary with the schema included

        Raises:
            SanityCheckError if no default schema can be found for this object type
        """
        if not self.default_schema:
            logger.warning("Unable to find default schemas and no schema provided")
            raise SanityCheckError(name, cleaned_dict, "default_schema")
        if object_type not in self.default_schema.keys():
            logger.warning(
                f"Unable to find default {object_type} schema and no schema provided"
            )
            raise SanityCheckError(name, cleaned_dict, "default schema")
        cleaned_dict["schema"] = self.default_schema[object_type]
        return cleaned_dict

    def smelt_object(
        self, cleaned_input: dict, object_keys: List[str]
    ) -> Tuple[dict, Optional[dict]] | None:
        """Split the cleaned input into an object dictionary and a parameter dictionary
        ready to be used to construct Pydantic models for validation"""
        object_dict = {}
        parameter_dict = {"parameters": []}
        if "schema" in cleaned_input.keys():
            parameter_dict["schema"] = cleaned_input.pop("schema")
        for key in cleaned_input.keys():
            if key in object_keys:
                object_dict[key] = cleaned_input[key]
            else:
                parameter_dict["parameters"].append(
                    {
                        "name": key,
                        "value": cleaned_input[key],
                    }
                )
        if not parameter_dict["parameters"]:
            return (object_dict, None)
        return (object_dict, parameter_dict)

    def smelt_project(
        self, cleaned_input: dict
    ) -> Tuple[RawProject, Optional[ParameterSet]] | None:
        """Inject the schema into the project dictionary if it's not
        already present. Do the same for an institution and convert to
        a RawProject dataclass for validation."""
        if not self.use_project(self):
            return None
        project_keys = PROJECT_KEYS
        if "project" in self.objects_with_ids:
            project_keys.append("persistent_id")
            project_keys.append("alternate_ids")
        if "schema" not in cleaned_input.keys():
            try:
                cleaned_input = self._inject_schema_from_default_value(
                    cleaned_input["name"], "project", cleaned_input
                )
            except SanityCheckError:
                return None
        if "institution" not in cleaned_input.keys():
            if self.default_institution:
                cleaned_input["institution"] = self.default_institution
            else:
                logger.warning(
                    "Unable to find default institution and no institution provided"
                )
                return None
        if isinstance(cleaned_input["institution"], str):
            cleaned_input["institution"] = [cleaned_input["institution"]]
        refined_object = self.smelt_object(cleaned_input, project_keys)
        if not refined_object:
            return None
        object_dict = refined_object[0]
        try:
            raw_project = RawProject.parse_obj(object_dict)
        except ValidationError:
            logger.warning(
                f"Unable to parse {object_dict} into a RawProject.",
                exc_info=True,
            )
            return None
        try:
            parameters = refined_object[1]
            try:
                parameters = ParameterSet.parse_obj(refined_object[1])
            except ValidationError:
                logger.warning(
                    f"Unable to parse {parameters} into a ParameterSet.",
                    exc_info=True,
                )
                parameters = None
        except IndexError:
            parameters = None
        return (raw_project, parameters)

    def smelt_experiment(
        self, cleaned_input: dict
    ) -> Tuple[RawExperiment, Optional[ParameterSet]] | None:
        """Inject the schema into the experiment dictionary if it's not
        already present.
        Convert to a RawExperiment dataclass for validation."""
        experiment_keys = EXPERIMENT_KEYS
        if "experiment" in self.objects_with_ids:
            experiment_keys.append("persistent_id")
            experiment_keys.append("alternate_ids")
        if "schema" not in cleaned_input.keys():
            try:
                cleaned_input = self._inject_schema_from_default_value(
                    cleaned_input["title"], "experiment", cleaned_input
                )
            except SanityCheckError:
                return None
        if "institution_name" not in cleaned_input.keys():
            cleaned_input["institution_name"] = self.default_institution
        if isinstance(cleaned_input["institution_name"], list):
            cleaned_input["institution_name"] = cleaned_input["institution_name"][0]
        refined_object = self.smelt_object(cleaned_input, experiment_keys)
        if not refined_object:
            return None
        object_dict = refined_object[0]
        try:
            raw_experiment = RawExperiment.parse_obj(object_dict)
        except ValidationError:
            logger.warning(
                f"Unable to parse {object_dict} into a RawExperiment.",
                exc_info=True,
            )
            return None
        try:
            parameters = refined_object[1]
            try:
                parameters = ParameterSet.parse_obj(refined_object[1])
            except ValidationError:
                logger.warning(
                    f"Unable to parse {parameters} into a ParameterSet.",
                    exc_info=True,
                )
                parameters = None
        except IndexError:
            parameters = None
        return (raw_experiment, parameters)

    def smelt_dataset(
        self, cleaned_input: dict
    ) -> Tuple[RawDataset, Optional[ParameterSet]] | None:
        """Inject the schema into the dataset dictionary if it's not
        already present.
        Convert to a RawDataset dataclass for validation."""
        dataset_keys = DATASET_KEYS
        if "dataset" in self.objects_with_ids:
            dataset_keys.append("persistent_id")
            dataset_keys.append("alternate_ids")
        if "schema" not in cleaned_input.keys():
            try:
                cleaned_input = self._inject_schema_from_default_value(
                    cleaned_input["description"], "dataset", cleaned_input
                )
            except SanityCheckError:
                return None
        refined_object = self.smelt_object(cleaned_input, dataset_keys)
        print(refined_object)
        if not refined_object:
            return None
        object_dict = refined_object[0]
        try:
            raw_dataset = RawDataset.parse_obj(object_dict)
        except ValidationError:
            logger.warning(
                f"Unable to parse {object_dict} into a RawDataset.",
                exc_info=True,
            )
            return None
        try:
            parameters = refined_object[1]
            try:
                parameters = ParameterSet.parse_obj(refined_object[1])
            except ValidationError:
                logger.warning(
                    f"Unable to parse {parameters} into a ParameterSet.",
                    exc_info=True,
                )
                parameters = None
        except IndexError:
            parameters = None
        return (raw_dataset, parameters)

    def _create_replica(
        self,
        relative_file_path: Path,
    ) -> DatafileReplica | None:
        """Create a datafile replica using the filepath and the storage
        box"""
        try:
            return DatafileReplica(
                uri=relative_file_path.as_posix(),
                location=self.storage_box.name,
                protocol="file",
            )
        except ValidationError:
            logger.warning(
                f"Unable to create a replica for {relative_file_path}",
                exc_info=True,
            )
            return None

    def smelt_datafile(self, cleaned_dict: dict) -> dict:
        """Wrapper to check and create the python dictionaries in a from expected by the
        forge class. The smelt_datafile function is somewhat unique as it takes a processed
        dictionary after having been through the rebase_file_path function and the
        expand_datafile_entry function processed by the Factory class. Arguably this should
        be refactored to be all held by the smelter class and an issue has been created in Github.

        Args:
            cleaned_dict: dictionary containing the object data and metadata

        Returns:
            A tuple containing an object dictionary, which is the minimum metadata required
            to create the object in MyTardis, and a parameter dictionary containing the
            additional metadata.
        """
        cleaned_dict = self.precondition_input_dictionary(cleaned_dict)
        object_keys = [
            "dataset",
            "filename",
            "md5sum",
            "directory",
            "mimetype",
            "size",
            "replicas",
            "users",
            "groups",
        ]
        if "schema" not in cleaned_dict.keys():
            try:
                cleaned_input = self._inject_schema_from_default_value(
                    cleaned_input["filename"], "datafile", cleaned_input
                )
            except SanityCheckError:
                return None
        try:
            relative_file_path = Path(cleaned_input.pop("relative_file_path"))
        except KeyError:
            logger.warning(
                (
                    f"Malformed input dictionary for datafile, {cleaned_input['filename']}. "
                    "Unable to find realtive_file_path so cannot create replica."
                ),
                exc_info=True,
            )
            return (None, None)
        cleaned_dict = self._create_replica(cleaned_dict)
        if not Smelter._verify_datafile(cleaned_dict):
            return (None, None)
        object_dict, parameter_dict = self._smelt_object(object_keys, cleaned_dict)
        object_dict["parameter_sets"] = parameter_dict
        return object_dict

    @staticmethod
    def _verify_datafile(cleaned_dict: dict) -> bool:
        """Function to make sure that there is a minimum set of
        data in the object dictionary to ensure that an object can be
        created in MyTardis

        Args:
            cleaned_dict: The dictionary that will be used to create
                the object in MyTardis

        Returns:
            Boolean value that shows if the minimum subset of data is present

        Raises:
            SanityCheckError if there is missing data
        """
        required_keys = [
            "dataset",
            "filename",
            "md5sum",
            "schema",
            "replicas",
        ]
        try:
            raw_datafile = RawDatafile.parse_obj(object_dict)
        except ValidationError:
            logger.warning(
                f"Unable to parse {object_dict} into a RawDatafile.",
                exc_info=True,
            )
            return None
        try:
            parameters = refined_object[1]
            if parameters:
                try:
                    parameters = ParameterSet.parse_obj(refined_object[1])
                except ValidationError:
                    logger.warning(
                        f"Unable to parse {parameters} into a ParameterSet.",
                        exc_info=True,
                    )
                    return None
            else:
                parameters = None
        except IndexError:
            parameters = None
        if parameters:
            raw_datafile.parameter_sets = parameters
        return raw_datafile

    @abstractmethod
    def get_input_file_paths(self, file_path: Path) -> List[Path]:  # pragma: no cover
        """
        Function to return a list of Paths containing all the relevant input
        files in the directory

        return ""  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def get_object_types_in_input_file(
        self, file_path: Path
    ) -> tuple:  # pragma: no cover
        """Function to read in an input file and return a tuple containing
        the object types that are in the file"""

        return tuple()  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def get_objects_in_input_file(
        self, file_path: Path, object_type: str
    ) -> list:  # pragma: no cover
        """Function to read in an input file and return a tuple containing
        the object types that are in the file"""

        return tuple()  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def read_file(self, file_path: Path) -> tuple:  # pragma: no cover
        """Function to read in different input files and process into
        dictionaries. As there may be more than one object in a file, wrap in
        a tuple and return."""

        return []  # pragma: no cover    @abstractmethod

    @abstractmethod
    def precondition_input(
        self, raw_input: dict, object_type: BaseObjectType
    ) -> dict | None:
        """Each concrete smelter class will need to process the metadata keys
        into appropriate formats for creating RawObject dataclasses and Optionally
        ObjectParameterSets.

        In addition all concrete preconditioning functions will need to call the
        parse_groups_and_users_from_separate_access function to deal with the user
        lists and put them into a normalised format.

        The concrete class method should also process the file_path into something
        that can be used by the __create_replica method to generate a replica.

        This will normally involve tidying up metadata keys
        """
        return {}
