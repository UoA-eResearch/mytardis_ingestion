"""Smelter base class. A class that provides functions to split input dictionaries into
dictionaries suited to be passed into an instance of the Forge class for creating
objects in MyTardis."""

import logging
from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path

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
        self.OBJECT_KEY_CONVERSION = self.get_key_conversions()
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

    @abstractmethod
    def get_key_conversions(self) -> dict:
        """Wrapper function to fill the OBJECT_KEY_CONVERSION dictionary"""
        return {}

    def tidy_up_dictionary_keys(self, parsed_dict: dict) -> dict:
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
            return (object_type, cleaned_dict)

        translation_dict = self.OBJECT_KEY_CONVERSION[object_type]
        for key in parsed_dict.keys():
            if key in translation_dict.keys():
                cleaned_dict[translation_dict[key]] = cleaned_dict.pop(key)
        return (object_type, cleaned_dict)

    def smelt_object(self, object_keys: list, cleaned_dict: dict) -> tuple:
        """A helper function to split up a combined dictionary into an object_dict
        and a parameter_dict ready for ingestion.

        Args:
            object_keys: a list of the keys that should be included in the object_dict
            cleaned_dict: a dictionary after the keys have been normalised

        Returns:
            A tuple containing the object_dict and parameter_dict
        """
        users, groups = Smelter.parse_groups_and_users_from_separate_access(
            cleaned_dict
        )
        object_type, cleaned_dict = self.tidy_up_dictionary_keys(cleaned_dict)
        cleaned_dict = self.tidy_up_metadata_keys(cleaned_dict, object_type)
        object_dict = {}
        if users != []:
            object_dict["users"] = users
        if groups != []:
            object_dict["groups"] = groups
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

    @abstractmethod
    def tidy_up_metadata_keys(self, cleaned_dict: dict, object_type: str) -> dict:
        """Placeholder function"""
        return None

    @staticmethod
    def parse_groups_and_users_from_separate_access(
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
        for user in combined_users:
            download = False
            sensitive = False
            if user in download_users:
                download = True
            if user in sensitive_users:
                sensitive = True
            users.append((user, False, download, sensitive))
        for group in combined_groups:
            download = False
            sensitive = False
            if group in download_groups:
                download = True
            if group in sensitive_groups:
                sensitive = True
            groups.append((group, False, download, sensitive))
        return (sorted(users), sorted(groups))

    def smelt_project(self, cleaned_dict: dict) -> tuple:
        if not self.projects_enabled:
            logger.warning(
                (
                    "MyTardis is not currently set up to use projects. Please check settings.py "
                    "and ensure that the 'projects' app is enabled. This may require rerunning "
                    "migrations."
                )
            )
            return (None, None)
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
        ]
        if "project" in self.objects_with_ids:
            object_keys.append("persistent_id")
            object_keys.append("alternate_ids")
        # NOTE: This is fragile and needs making more robust
        if (
            "schema" not in cleaned_dict.keys()
            and "project" in self.default_schema.keys()
        ):
            cleaned_dict["schema"] = self.default_schema["project"]
        if "institution" not in cleaned_dict.keys() and self.default_institution:
            cleaned_dict["institution"] = self.default_institution
        return self.smelt_object(object_keys, cleaned_dict)

    def smelt_experiment(self, cleaned_dict: dict) -> tuple:
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
        ]
        if "experiment" in self.objects_with_ids:
            object_keys.append("persistent_id")
            object_keys.append("alternate_ids")
        if self.projects_enabled:
            object_keys.append("projects")
        # NOTE: This is fragile and needs making more robust
        if (
            "schema" not in cleaned_dict.keys()
            and "experiment" in self.default_schema.keys()
        ):
            cleaned_dict["schema"] = self.default_schema["experiment"]
        return self.smelt_object(object_keys, cleaned_dict)

    def smelt_dataset(self, cleaned_dict: dict) -> tuple:
        object_keys = [
            "experiments",
            "description",
            "directory",
            "created_time",
            "modified_time",
            "immutable",
            "instrument",
        ]
        if "dataset" in self.objects_with_ids:
            object_keys.append("persistent_id")
            object_keys.append("alternate_ids")
        # NOTE: This is fragile and needs making more robust
        if (
            "schema" not in cleaned_dict.keys()
            and "dataset" in self.default_schema.keys()
        ):
            cleaned_dict["schema"] = self.default_schema["dataset"]
        return self.smelt_object(object_keys, cleaned_dict)

    def smelt_datafile(self, cleaned_dict: dict) -> tuple:
        object_keys = [
            "dataset",
            "filename",
            "md5sum",
            "directory",
            "mimetype",
            "size",
            "replicas",
        ]
        # NOTE: This is fragile and needs making more robust
        if (
            "schema" not in cleaned_dict.keys()
            and "datafile" in self.default_schema.keys()
        ):
            cleaned_dict["schema"] = self.default_schema["datafile"]
        object_dict, parameter_dict = self.smelt_object(object_keys, cleaned_dict)
        return (object_dict, parameter_dict)

    def create_replica(self, cleaned_dict: dict) -> dict:
        uri = cleaned_dict.pop("file_path")
        location = self.storage_box
        replica = {"uri": uri.as_posix(), "location": location, "protocol": "file"}
        cleaned_dict["replicas"] = [replica]
        return cleaned_dict

    def verify_project(self, cleaned_dict: dict) -> bool:
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
            # TODO Log error here
            return False
        return False

    def verify_experiment(self, cleaned_dict: dict) -> bool:
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
            # TODO Log error here
            return False
        return False

    def verify_dataset(self, cleaned_dict: dict) -> bool:
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
            # TODO Log error here
            return False
        return False

    def verify_datafile(self, cleaned_dict: dict) -> bool:
        required_keys = [
            "dataset",
            "filename",
            "md5sum",
            "storage_box",
            "schema",
            "file_path",
        ]
        try:
            return sanity_check("datafile", cleaned_dict, required_keys)
        except SanityCheckError as error:
            # TODO Log error here
            return False
        return False

    @abstractmethod
    def get_file_type_for_input_files(self) -> str:
        """Function to return a string that can be used by Path.glob() to
        get all of the input files in a directory"""

        return ""

    @abstractmethod
    def get_objects_in_input_file(self, file_path: Path) -> tuple:
        """Function to read in an input file and return a tuple containing
        the object types that are in the file"""

        return tuple(file_path)

    @abstractmethod
    def read_file(self, file_path: Path) -> tuple:
        """Function to read in different input files and process into
        dictionaries. As there may be more than one object in a file, wrap in
        a tuple and return."""

        return tuple(file_path)

    @abstractmethod
    def rebase_file_path(self, parsed_dict: dict) -> dict:
        """Function to fix up the file path to take into account where it is
        mounted."""
        return parsed_dict

    @abstractmethod
    def expand_datafile_entry(self, parsed_dict) -> list:
        """Function to add the additional fields (size, checksum etc.) from files"""
        return []

    @abstractmethod
    def get_object_from_dictionary(self, parsed_dict: dict) -> str:
        """Helper function to get the object type from a parsed dictionary"""
        return None

    @staticmethod
    def get_filename_from_filepath(file_path: Path) -> str:
        return file_path.name

    @staticmethod
    def get_file_size(file_path: Path) -> int:
        return file_path.stat().st_size

    @staticmethod
    def get_file_checksum(file_path: Path) -> int:
        return calculate_md5sum(file_path)
