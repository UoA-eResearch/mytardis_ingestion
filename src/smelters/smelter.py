# pylint: disable=logging-fstring-interpolation,consider-iterating-dictionary
"""Smelter base class. A class that provides functions to split input dictionaries into
dictionaries suited to be passed into an instance of the Forge class for creating
objects in MyTardis."""

import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import Union

from src.helpers import SanityCheckError, calculate_md5sum, sanity_check

logger = logging.getLogger(__name__)


class Smelter(ABC):
    """The Smelter base class to be subclassed into individual concrete classes for different
    ingestion approaches.

    Smelter classes share a number of similar processing routines, especially around dictionary
    modification and date handling.

    Attributes:

    """

    def __init__(self, mytardis_config: dict) -> None:
        """Class initialisation to set options for dictionary processing

        Stores MyTardis set up information from the introspection API to allow the parser
        to prepare only objects that exist and to handle additional keys in the dictionaries
        gracefully.

        Args:
            projects_enabled: a boolean flag indicating whether or not to process projects
            objects_with_pids: a list of objects that have identifiers. Defaults to empty
            objects_with_profiles: a list of objects that have profiles. Defaults to empty
            default_schema: a dictionary of schema namespaces to use for projects,
                experiments, datasets and datafiles
        """
        self.projects_enabled = mytardis_config["projects_enabled"]
        try:
            self.objects_with_ids = mytardis_config["objects_with_ids"]
        except KeyError:
            self.objects_with_ids = []
        try:
            self.objects_with_profiles = mytardis_config["objects_with_profiles"]
        except KeyError:
            self.objects_with_profiles = []
        self.OBJECT_KEY_CONVERSION = (  # pylint: disable=invalid-name
            self.get_key_conversions()
        )
        try:
            self.default_schema = mytardis_config["default_schema"]
        except KeyError:
            self.default_schema = None
        try:
            self.default_institution = mytardis_config["default_institution"]
        except KeyError:
            self.default_institution = None
        self.mount_dir = Path(mytardis_config["mount_directory"])
        self.remote_dir = Path(mytardis_config["remote_directory"])
        self.storage_box = mytardis_config["storage_box"]

    def tidy_up_dictionary_keys(self, parsed_dict: dict) -> tuple:
        """A helper function to ensure that the dictionary keys in a parsed dictionary
        are set to their appropriate values for passing to MyTardis.

        Args:
            parsed_dict: The raw dictionary parsed by the file reader or determined from
                an external source such as a directory structure

        Returns:
            a cleaned dictionary with the various keys altered to match those in the
                MyTardis models.
        """
        cleaned_dict = deepcopy(parsed_dict)
        object_type = self.get_object_from_dictionary(cleaned_dict)
        try:
            if self.OBJECT_KEY_CONVERSION[object_type] == {}:
                return (object_type, cleaned_dict)
        except KeyError:
            logger.warning(
                f"Unable to find {object_type} in OBJECT_KEY_CONVERSION dictionary"
            )
            return (object_type, cleaned_dict)
        translation_dict = self.OBJECT_KEY_CONVERSION[object_type]
        for key in parsed_dict.keys():
            if key in translation_dict.keys():
                cleaned_dict[translation_dict[key]] = cleaned_dict.pop(key)
        return (object_type, cleaned_dict)

    def _smelt_object(  # pylint: disable=consider-using-dict-items,no-self-use
        self, object_keys: list, cleaned_dict: dict
    ) -> tuple:
        """A helper function to split up a combined dictionary into an object_dict
        and a parameter_dict ready for ingestion.

        Args:
            object_keys: a list of the keys that should be included in the object_dict
            cleaned_dict: a dictionary after the keys have been normalised

        Returns:
            A tuple containing an object dictionary, which is the minimum metadata required
            to create the object in MyTardis, and a parameter dictionary containing the
            additional metadata.
        """
        if isinstance(object_keys, str):
            object_keys = [object_keys]
        object_dict = {}
        parameter_dict = {}
        schema = cleaned_dict.pop("schema")
        parameter_dict["schema"] = schema
        parameter_dict["parameters"] = []
        for key in cleaned_dict.keys():
            if key in object_keys:
                object_dict[key] = cleaned_dict[key]
            else:
                parameter_dict["parameters"].append(
                    {"name": key, "value": cleaned_dict[key]}
                )
        return (object_dict, parameter_dict)

    @staticmethod
    def set_access_controls(
        combined_names: set, download_names: list, sensitive_names: list
    ) -> list:
        """Helper function to set the access controls for combined list of
        users/groups.

        Args:
            combined_names: a set of names of users/groups that is complete for this
                ingestion object.
            download_names: a list of names of users/groups that have download access
            sensitive_names: a list of names of users/groups that have sensitive access

        Returns:
            A list of tuples containing the access controls.
        """
        if isinstance(combined_names, str):
            combined_names = [combined_names]
        if isinstance(download_names, str):
            combined_names = [download_names]
        if isinstance(sensitive_names, str):
            combined_names = [sensitive_names]
        return_list = []
        for name in combined_names:
            download = False
            sensitive = False
            if name in download_names:
                download = True
            if name in sensitive_names:
                sensitive = True
            return_list.append((name, False, download, sensitive))
        return return_list

    @staticmethod
    def parse_groups_and_users_from_separate_access(  # pylint: disable=too-many-branches
        cleaned_dict: dict,
    ) -> tuple:
        """A helper function to parse a set of separate users and groups into a single
        list of users and groups with access levels included

        The output format is (user/group, isOwner, hasDownload, hasSensitive)

        Args:
            cleaned_dict: a dictionary after the keys have been normalised

        Returns:
            A tuple containing the lists of users and groups with access levels
        """
        groups = []
        users = []
        read_groups = []
        read_users = []
        download_groups = []
        download_users = []
        sensitive_groups = []
        sensitive_users = []
        if "principal_investigator" in cleaned_dict.keys():
            if "admin_users" in cleaned_dict.keys():
                cleaned_dict["admin_users"].append(
                    cleaned_dict["principal_investigator"]
                )
            else:
                cleaned_dict["admin_users"] = [
                    cleaned_dict["principal_investigator"]]
        if "admin_users" in cleaned_dict.keys():
            for user in set(cleaned_dict.pop("admin_users")):
                users.append((user, True, True, True))
        if "admin_groups" in cleaned_dict.keys():
            for group in set(cleaned_dict.pop("admin_groups")):
                groups.append((group, True, True, True))
        try:
            read_users = cleaned_dict.pop("read_users")
        except KeyError:  # not present in the dictionary so ignore
            pass
        try:
            read_groups = cleaned_dict.pop("read_groups")
        except KeyError:  # not present in the dictionary so ignore
            pass
        try:
            download_users = cleaned_dict.pop("download_users")
        except KeyError:  # not present in the dictionary so ignore
            pass
        try:
            download_groups = cleaned_dict.pop("download_groups")
        except KeyError:  # not present in the dictionary so ignore
            pass
        try:
            sensitive_users = cleaned_dict.pop("sensitive_users")
        except KeyError:  # not present in the dictionary so ignore
            pass
        try:
            sensitive_groups = cleaned_dict.pop("sensitive_groups")
        except KeyError:  # not present in the dictionary so ignore
            pass
        combined_users = set(read_users + download_users + sensitive_users)
        combined_groups = set(read_groups + download_groups + sensitive_groups)
        users.extend(
            Smelter.set_access_controls(
                combined_users, download_users, sensitive_users)
        )
        groups.extend(
            Smelter.set_access_controls(
                combined_groups, download_groups, sensitive_groups
            )
        )
        return (sorted(users), sorted(groups))

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
        """Wrapper to check and create the python dictionaries in a from expected by the
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
        if "project" in self.objects_with_ids:
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
            object_type: the object type defined by the dictionary
            cleaned_dict: The dictionary to inject schema into

        Returns:
            the dictionary with the schema included

        Raises:
            SanityCheckError if no default schema can be found for this object type
        """
        if object_type not in self.default_schema.keys():
            raise SanityCheckError(name, cleaned_dict, "default schema")
        cleaned_dict["schema"] = self.default_schema[object_type]
        return cleaned_dict

    def _verify_project(self, cleaned_dict: dict) -> bool:
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
            "name",
            "description",
            "schema",
            "principal_investigator",
            "institution",
        ]
        if "project" in self.objects_with_ids:
            required_keys.append("persistent_id")
        try:
            return sanity_check("project", cleaned_dict, required_keys)
        except SanityCheckError as error:
            logger.warning(
                (
                    "Incomplete data for Project creation\n"
                    f"cleaned_dict: {cleaned_dict}\n"
                    f"missing keys: {sorted(error.missing_keys)}"
                )
            )
            return False
        return False

    def smelt_experiment(self, cleaned_dict: dict) -> tuple:
        """Wrapper to check and create the python dictionaries in a from expected by the
        forge class.

        Args:
            cleaned_dict: dictionary containing the object data and metadata

        Returns:
            A tuple containing an object dictionary, which is the minimum metadata required
            to create the object in MyTardis, and a parameter dictionary containing the
            additional metadata.
        """
        cleaned_dict = self.precondition_input_dictionary(cleaned_dict)
        object_keys = [
            "title",
            "description",
            "institution_name",
            "locked",
            "public_access",
            "institution",
            "embargo_until",
            "start_time",
            "end_time",
            "created_time",
            "update_time",
            "created_by",
            "url",
            "users",
            "groups",
        ]
        if "experiment" in self.objects_with_ids:
            object_keys.append("persistent_id")
            object_keys.append("alternate_ids")
        if self.projects_enabled:
            object_keys.append("projects")
        if "schema" not in cleaned_dict.keys():
            try:
                cleaned_dict = self._inject_schema_from_default_value(
                    cleaned_dict["title"], "experiment", cleaned_dict
                )
            except SanityCheckError:
                logger.warning(
                    "Unable to find default experiment schema and no schema provided"
                )
                return (None, None)
        if not self._verify_experiment(cleaned_dict):
            return (None, None)
        return self._smelt_object(object_keys, cleaned_dict)

    def _verify_experiment(self, cleaned_dict: dict) -> bool:
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
            "title",
            "description",
            "schema",
        ]
        if "experiment" in self.objects_with_ids:
            required_keys.append("persistent_id")
        if self.projects_enabled:
            required_keys.append("projects")
        try:
            return sanity_check("experiment", cleaned_dict, required_keys)
        except SanityCheckError as error:
            logger.warning(
                (
                    "Incomplete data for Experiment creation\n"
                    f"cleaned_dict: {cleaned_dict}\n"
                    f"missing keys: {error.missing_keys}"
                )
            )
            return False
        return False

    def smelt_dataset(self, cleaned_dict: dict) -> tuple:
        """Wrapper to check and create the python dictionaries in a from expected by the
        forge class.

        Args:
            cleaned_dict: dictionary containing the object data and metadata

        Returns:
            A tuple containing an object dictionary, which is the minimum metadata required
            to create the object in MyTardis, and a parameter dictionary containing the
            additional metadata.
        """
        cleaned_dict = self.precondition_input_dictionary(cleaned_dict)
        object_keys = [
            "experiments",
            "description",
            "directory",
            "created_time",
            "modified_time",
            "immutable",
            "instrument",
            "users",
            "groups",
        ]
        if "dataset" in self.objects_with_ids:
            object_keys.append("persistent_id")
            object_keys.append("alternate_ids")
        if "schema" not in cleaned_dict.keys():
            try:
                cleaned_dict = self._inject_schema_from_default_value(
                    cleaned_dict["description"], "dataset", cleaned_dict
                )
            except SanityCheckError:
                logger.warning(
                    "Unable to find default dataset schema and no schema provided"
                )
                return (None, None)
        if not self._verify_dataset(cleaned_dict):
            return (None, None)
        return self._smelt_object(object_keys, cleaned_dict)

    def _verify_dataset(self, cleaned_dict: dict) -> bool:
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
            "description",
            "schema",
            "experiments",
            "instrument",
        ]
        if "dataset" in self.objects_with_ids:
            required_keys.append("persistent_id")
        try:
            return sanity_check("dataset", cleaned_dict, required_keys)
        except SanityCheckError as error:
            logger.warning(
                (
                    "Incomplete data for Dataset creation\n"
                    f"cleaned_dict: {cleaned_dict}\n"
                    f"missing keys: {error.missing_keys}"
                )
            )
            return False
        return False

    def smelt_datafile(self, cleaned_dict: dict) -> tuple:
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
                cleaned_dict = self._inject_schema_from_default_value(
                    cleaned_dict["filename"], "datafile", cleaned_dict
                )
            except SanityCheckError:
                logger.warning(
                    "Unable to find default datafile schema and no schema provided"
                )
                return (None, None)
        try:
            _ = cleaned_dict["file_path"]
        except KeyError:
            logger.warning(
                f"Unable to create file replica for {cleaned_dict['filename']}"
            )
            return (None, None)
        cleaned_dict = self._create_replica(cleaned_dict)
        if not Smelter._verify_datafile(cleaned_dict):
            return (None, None)
        object_dict, parameter_dict = self._smelt_object(
            object_keys, cleaned_dict)
        return (object_dict, parameter_dict)

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
            return sanity_check("datafile", cleaned_dict, required_keys)
        except SanityCheckError as error:
            logger.warning(
                (
                    "Incomplete data for Datafile creation\n"
                    f"cleaned_dict: {cleaned_dict}\n"
                    f"missing keys: {error.missing_keys}"
                )
            )
            return False
        return False

    def _create_replica(self, cleaned_dict: dict) -> dict:
        """Function to create a MyTardis replica based on the file_path
        passed in the cleaned dictionary

        Args:
            cleaned_dict: a dictionary containing the file_path key word

        Returns:
            The cleaned_dict including the replica ready for ingestion
        """
        uri = cleaned_dict.pop("file_path")
        location = self.storage_box
        replica = {"uri": uri.as_posix(), "location": location,
                   "protocol": "file"}
        cleaned_dict["replicas"] = [replica]
        return cleaned_dict

    @staticmethod
    def get_filename_from_filepath(  # pylint: disable=missing-function-docstring
        file_path: Path,
    ) -> str:
        """Wrapper around Pathlib Path .name attribute for disambiguation"""
        return file_path.name  # pragma: no cover

    @staticmethod
    def get_file_size(  # pylint: disable=missing-function-docstring
        file_path: Path,
    ) -> int:
        """Wrapper around Pathlib Path .stat().size attribute for diambiguation"""
        return file_path.stat().st_size  # pragma: no cover

    @staticmethod
    def get_file_checksum(  # pylint: disable=missing-function-docstring
        file_path: Path,
    ) -> int:
        """Wrapper around calculate_md5sum function for disambiguation"""
        return calculate_md5sum(file_path)

    @abstractmethod
    def get_key_conversions(self) -> dict:
        """Wrapper function to fill the OBJECT_KEY_CONVERSION dictionary"""
        return {}

    @abstractmethod
    def _tidy_up_metadata_keys(
        self, parsed_dict: dict, object_type: str
    ) -> dict:  # pragma: no cover
        """Placeholder function"""
        return {}

    @abstractmethod
    def get_input_file_paths(self, file_path: Path) -> list[Path]:  # pragma: no cover
        """
        Function to return a list of Paths containing all the relevant input
        files in the directory

        Args:
            file_path: a Path object pointing to the directory containing the
            relevant input files

        Returns:
            A list of Path objects, each element pointing to one possible input
            file
        """

        return [Path()]

    @abstractmethod  # pragma: no cover
    def get_objects_in_input_file(self, file_path: Path) -> tuple:  # pragma: no cover
        """Function to read in an input file and return a tuple containing
        the object types that are in the file"""

        return tuple()  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def read_file(self, file_path: Path) -> tuple:  # pragma: no cover
        """Function to read in different input files and process into
        dictionaries. As there may be more than one object in a file, wrap in
        a tuple and return."""

        return tuple()  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def rebase_file_path(self, parsed_dict: dict) -> dict:  # pragma: no cover
        """Function to fix up the file path to take into account where it is
        mounted."""
        return parsed_dict  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def expand_datafile_entry(self, parsed_dict) -> list:  # pragma: no cover
        """Function to add the additional fields (size, checksum etc.) from files"""
        return []  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def get_object_from_dictionary(self, parsed_dict: dict) -> Union[str, None]:
        """Helper function to get the object type from a parsed dictionary"""
        return None  # pragma: no cover
