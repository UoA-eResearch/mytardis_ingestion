# pylint: disable=consider-using-set-comprehension,logging-fstring-interpolation,unnecessary-comprehension,R0801,fixme
"""IngestionFactory is a base class for specific instances of MyTardis
Ingestion scripts. The base class contains mostly concrete functions but
needs to determine the Smelter class that is used by the Factory"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

from src.crucible import Crucible
from src.forges import Forge
from src.helpers import match_object_with_dictionary
from src.overseers import Overseer

logger = logging.getLogger(__name__)

PARENT_TYPE_TO_KEY_CONVERSTIONS = {  # pylint disable=invalid_name
    "project": "projects",
    "experiment": "experiments",
    "dataset": "dataset",
}

NAME_CONVERSTIONS = {  # pylint disable=invalid_name
    "project": "name",
    "experiment": "title",
    "dataset": "description",
    "datafile": "filename",
}


class IngestionFactory(ABC):
    """Ingestion Factory base class to orchestrate the Smelting, and Forging of
    objects within MyTardis.

    IngestionFactory is an abstract base class from which specific ingestion
    factories should be subclassed. The Factory classes orchestrate the various
    classes associated with ingestion in such a way that the Smelter, Overseer and Forge
    classes are unaware of each other

    Attributes:
        self.overseer: An instance of the Overseer class
        self.mytardis_setup: The return from the introspection API that specifies how MyTardis is
            up Set.
        Self.forge: An instance of the Forge class
        self.smelter: An instance of a smelter class that varies for different ingestion approaches
        self.default_institution: Either a name, or an identifier for an Institution to use as the
            default for Project and Experiment creation.
    """

    def __init__(self, config_dict: dict) -> None:
        """Initialises the Factory with the configuration found in the config_dict.

        Passes the config_dict to the overseer and forge instances to ensure that the
        specific MyTardis configuration is shared across all classes

        Args:
           config_dict: A configuration dictionary containing the keys required to
                initialise the IngestionFactory and it's sub-classes. See documenation
                for more details.
        """
        self.overseer = Overseer(config_dict)
        self.crucible = Crucible(config_dict)
        self.mytardis_setup = self.overseer.get_mytardis_set_up()
        config_dict.update(self.mytardis_setup)
        self.forge = Forge(config_dict)
        self.smelter = self.get_smelter(config_dict)
        self.default_institution = config_dict["default_institution"]
        self.object_dictionary = {}
        self.blocked_ids: dict = {
            "experiment": [],
            "dataset": [],
            "datafile": [],
        }
        self.experiment_comparison_keys: list = [
            "title",
            "description",
        ]
        self.dataset_comparison_keys: list = [
            "description",
        ]
        self.datafile_comparison_keys: list = [
            "filename",
            "size",
            "md5sum",
        ]
        if self.mytardis_setup["projects_enabled"]:
            self.blocked_ids["project"] = []
            self.project_comparison_keys: list = [
                "name",
                "description",
                "principal_investigator",
            ]

    @abstractmethod  # pragma: no cover
    def get_smelter(self, config_dict: dict):  # pragma: no cover
        """Abstract method to return the specific instance of a smelter for the
        concrete instance of the IngestionFactory.

        Returns:
            None
        """
        return None  # pragma: no cover

    def _build_object_dict(
        self,
        input_file_directory: Path,
        object_types: Union[list, None] = None,
    ) -> dict:
        """Function to iterate through a list of input files and return a dictionary of
        files that have objects defined within them, keyed to the object type

        Args:
            input_file_directory: A Path object pointing to the parent directory that contains
                the input files for creating the MyTardis objects.
            object_types: A tuple of object types that are being sought.

        Returns:
            A list of files that have inputs of type object_type
        """
        return_dict: dict = {}
        if not object_types:
            object_types = [
                "project",
                "experiment",
                "dataset",
                "datafile",
            ]
        file_paths = self.smelter.get_input_file_paths(input_file_directory)
        for object_type in object_types:
            return_dict[object_type] = []
        for file_path in file_paths:
            objects = self.smelter.get_object_types_in_input_file(file_path)
            objects = [object_types for object_types in objects]
            for object_type in objects:
                return_dict[object_type].append(file_path)
        return return_dict

    def _get_object_from_mytardis_if_it_already_exists(
        self,
        object_type: str,
        search_target: str,
        search_string: str,
    ) -> Union[list, None]:
        """Attempt to get a list of objects matched in MyTardis using their identifiers, if enabled
        or a defined search target.

        Args:
            object_type: the string representation of the object type
            search_target: the field name in MyTardis to search on.
            search_string: the string containing the value to be searched for in MyTardis

        Returns:
            A list of known matches, an empty list if no matches are found, or None if there is an
                error in the search
        """
        if object_type in self.mytardis_setup["objects_with_ids"]:
            objects_from_mytardis = self.overseer.get_objects(
                object_type,
                "pids",
                search_string,
            )
        if not objects_from_mytardis:
            objects_from_mytardis = self.overseer.get_objects(
                object_type,
                search_target,
                search_string,
            )
        return objects_from_mytardis

    def get_project_from_mytardis_if_it_already_exists(
        self,
        search_string: str,
    ) -> Union[list, None]:
        """Helper function to get projects from MyTardis

        Args:
            search_string: The string used to find objects in MyTardis

        Returns:
            A list of known matches, an empty list if no matches are found, or None if there is an
                error in the search
        """
        return self._get_object_from_mytardis_if_it_already_exists(
            "project",
            "name",
            search_string,
        )

    def get_experiment_from_mytardis_if_it_already_exists(
        self,
        search_string: str,
    ) -> Union[list, None]:
        """Helper function to get experiments from MyTardis

        Args:
            search_string: The string used to find objects in MyTardis

        Returns:
            A list of known matches, an empty list if no matches are found, or None if there is an
                error in the search
        """
        return self._get_object_from_mytardis_if_it_already_exists(
            "experiment",
            "title",
            search_string,
        )

    def get_dataset_from_mytardis_if_it_already_exists(
        self,
        search_string: str,
    ) -> Union[list, None]:
        """Helper function to get datasets from MyTardis

        Args:
            search_string: The string used to find objects in MyTardis

        Returns:
            A list of known matches, an empty list if no matches are found, or None if there is an
                error in the search
        """
        return self._get_object_from_mytardis_if_it_already_exists(
            "dataset",
            "description",
            search_string,
        )

    def _verify_object_unblocked(
        self,
        object_type: str,
        object_id: str,
    ) -> bool:
        """Function to compare an object against the known list of objects that have been
        blocked.

        Args:
            object_type: The string representation of the parent object type
            object_id: The string used to identify the parent object

        Returns:
            True if the object is not present in the list of known blocked objects,
                otherwise False.
        """
        if object_id in self.blocked_ids[object_type]:
            return False
        return True

    def _block_object(
        self,
        object_type: str,
        object_dictionary: dict,
    ) -> None:
        """Helper function to add all known identifiers for an object to the blocked_ids
        dictionary when the object has been blocked

        Args:
            object_type: the string representation of the object type in MyTardis
            object_dictionary: a dictionary containing the object to be created in MyTardis
        """
        name = object_dictionary[NAME_CONVERSTIONS[object_type]]
        self.blocked_ids[object_type].append(name)
        if object_type in self.mytardis_setup["objects_with_ids"]:
            if (  # added because not all objects will have a persistent ID
                "persistent_id" in object_dictionary.keys()
                and object_dictionary["persistent_id"]
            ):
                self.blocked_ids[object_type].append(object_dictionary["persistent_id"])
            if (  # Not all objects will have alternate_ids either
                "alternate_ids" in object_dictionary.keys()
                and object_dictionary["alternate_ids"]
            ):
                for identifier in object_dictionary["alternate_ids"]:
                    self.blocked_ids[object_type].append(identifier)

    def _match_or_block_object(
        self,
        object_type: str,
        object_dictionary: dict,
        mytardis_dictionary: dict,
        comparison_keys: list,
    ) -> Union[str, None]:
        """Generic function to match objects or block them if a partial match is made,

        Args:
            object_type: The string representation of the object being matched
            object_dictionary: The dictionary generated by the smelter for ingestion
            mytardis_dictionary: The dictionary returned by MyTardis that is a match or
                partial match
            comparison_keys: A list of keys that should be used to compare the two dictionaries

        Returns:
            None if the match is a partial match. This also blocks the ID against futher
                ingestion and prevents child objects being ingested in this run
            The URI from MyTardis as a string if a complete match is made.
        """

        match = match_object_with_dictionary(
            object_dictionary,
            mytardis_dictionary,
            comparison_keys,
        )
        if match:
            try:
                uri = mytardis_dictionary["resource_uri"]
            except KeyError:
                logger.warning(
                    (
                        f"Unable to find the resource_uri field in the mytardis {object_type} "
                        "dictionary. This suggests an incomplete or malformed dictionary. The "
                        f"dictionary passed was: {mytardis_dictionary}. Blocking the "
                        "ID until this error can be checked."
                    )
                )
                self._block_object(object_type, object_dictionary)
                return None
            return uri
        logger.warning(
            (
                "Mismatch in dictionaries. Since we are unable to uniquely identify the "
                f"{object_type}, and given the potential for mis-assigning sensitive data "
                f"this {object_type} object will not be created. The ID will also be blocked "
                f"during this ingestion run.\nObject dictionary: {object_dictionary}\n"
                f"MyTardis near match: {mytardis_dictionary}."
            )
        )
        self._block_object(object_type, object_dictionary)
        return None

    def _remove_blocked_parents_from_object_dict(
        self,
        parent_type: str,
        object_type: str,
        object_dictionary: dict,
    ) -> Union[dict, None]:
        """Function to remove blocked parents from the object dictionary.

        Args:
            parent_type: the string representation of the parent object type
            object_type: the string representation of the object type
            object_dictionary: the ingestion dictionary that defines the object to be
                constructed in MyTardis

        Returns:
            The dictionary with any blocked parents removed or None if there are no
                parents that remain unblocked
        """
        parents = object_dictionary[PARENT_TYPE_TO_KEY_CONVERSTIONS[parent_type]]
        parents = list(set(parents))  # remove duplicates
        blocked = []
        for parent in parents:
            if not self._verify_object_unblocked(parent_type, parent):
                blocked.append(parent)
        unblocked = list(set(parents) - set(blocked))
        if blocked:
            logger.warning(
                (
                    f"Some parents of the {object_type} "
                    f"{object_dictionary[NAME_CONVERSTIONS[object_type]]} have been removed "
                    "due to previously being blocked. The parents removed are "
                    f"{list(set(blocked))}"
                )
            )
        if unblocked:
            object_dictionary[PARENT_TYPE_TO_KEY_CONVERSTIONS[parent_type]] = unblocked
            return object_dictionary
        return None

    def match_or_block_project(
        self,
        object_dictionary: dict,
        mytardis_dictionary: dict,
    ) -> Union[None, str]:
        """Function that compares two dictionaries using the match_object_with_dictionary
        function from the helpers module. If a perfect match can't be found then block
        the project_id for cascading purposes.

        Args:
            object_dictionary: the ingestion dictionary that has matched with a hit from MyTardis
            mytardis_dictionary: the resulting dictionary from MyTardis that has matched

        Returns:
            The uri as a string if the match is perfect, otherwise None and adds the id to the
                self.blocked_ids for project
        """
        if not self.mytardis_setup["projects_enabled"]:
            logger.warning(
                (
                    "Projects have not been enabled for this instance of MyTardis. Please contact "
                    "your MyTardis sysadmin for further information."
                )
            )
            return None
        comparison_keys = self.project_comparison_keys
        project_id = object_dictionary["name"]
        if "project" in self.mytardis_setup["objects_with_ids"]:
            try:
                project_id = object_dictionary["persistent_id"]
            except KeyError:
                logger.warning(
                    (f"No persistent ID found for project {object_dictionary['name']}")
                )
        if self._verify_object_unblocked("project", object_dictionary["name"]):
            match = self._match_or_block_object(
                "project",
                object_dictionary,
                mytardis_dictionary,
                comparison_keys,
            )
            return match
        logger.warning(
            (
                f"The project_id, {project_id} has been previously blocked for ingestion due "
                "to a mismatch. Please refer to the log files to identify the cause of this issue."
            )
        )
        return None

    def match_or_block_experiment(
        self,
        object_dictionary: dict,
        mytardis_dictionary: dict,
    ) -> Union[None, str]:
        """Function that compares two dictionaries using the match_object_with_dictionary
        function from the helpers module. If a perfect match can't be found then block
        the experiment_id for cascading purposes.

        Note: This function will also block an experiment on the basis of it's project_id
        if that ID is found in the blocked projects. This is to ensure that we don't have the
        case in which data is appended to an incorrect project

        Args:
            object_dictionary: the ingestion dictionary that has matched with a hit from MyTardis
            mytardis_dictionary: the resulting dictionary from MyTardis that has matched

        Returns:
            The uri as a string if the match is perfect, otherwise None and adds the id to the
                self.blocked_ids for experiment
        """
        if self.mytardis_setup["projects_enabled"]:
            try:
                _ = object_dictionary["projects"]
            except KeyError:
                logger.warning(
                    (
                        "Experiment dictionary is missing a project_id to link it to it's "
                        f"parent project. Object dictionary in question is {object_dictionary}"
                    )
                )
                return None
            cleaned_dict = self._remove_blocked_parents_from_object_dict(
                "project",
                "experiment",
                object_dictionary,
            )
            if cleaned_dict is None:
                logger.warning(
                    (
                        f"There are no valid parents for experiment {object_dictionary['title']}. "
                        "Blocking the experiment. Please check the Project details and "
                        "reingest."
                    )
                )
                self._block_object("experiment", object_dictionary)
        else:
            if "projects" in object_dictionary.keys():
                logger.warning(
                    (
                        "The Experiment ingestion dictionary contains project IDs to link to. "
                        "This instance of MyTardis is not set up to use projects. Please contact "
                        "your SysAdmin to enable projects. The experiment in question is defined "
                        f"by {object_dictionary['title']}."
                    )
                )
        comparison_keys = self.experiment_comparison_keys
        experiment_id = object_dictionary["title"]
        if "experiment" in self.mytardis_setup["objects_with_ids"]:
            try:
                experiment_id = object_dictionary["persistent_id"]
            except KeyError:
                logger.warning(
                    (
                        f"No persistent ID found for experiment {object_dictionary['title']}"
                    )
                )
        if self._verify_object_unblocked("experiment", experiment_id):
            match = self._match_or_block_object(
                "experiment",
                object_dictionary,
                mytardis_dictionary,
                comparison_keys,
            )
            return match
        logger.warning(
            (
                f"The experiment_id, {experiment_id} has been blocked for ingestion due to a "
                "mismatch. Please refer to the log files to identify the cause of this issue."
            )
        )
        return None

    def match_or_block_dataset(
        self,
        object_dictionary: dict,
        mytardis_dictionary: dict,
    ) -> Union[None, str]:
        """Function that compares two dictionaries using the match_object_with_dictionary
        function from the helpers module. If a perfect match can't be found then block
        the dataset_id for cascading purposes.

        Note: This function will also block a dataset on the basis of it's experiment_id
        if that ID is found in the blocked experiments. This is to ensure that we don't have the
        case in which data is appended to an incorrect experiment

        Args:
            object_dictionary: the ingestion dictionary that has matched with a hit from MyTardis
            mytardis_dictionary: the resulting dictionary from MyTardis that has matched

        Returns:
            The uri as a string if the match is perfect, otherwise None and adds the id to the
                self.blocked_ids for dataset
        """
        cleaned_dict = self._remove_blocked_parents_from_object_dict(
            "experiment",
            "dataset",
            object_dictionary,
        )
        if cleaned_dict is None:
            logger.warning(
                (
                    f"There are no valid parents for dataset {object_dictionary['description']}. "
                    "Blocking the dataset. Please check the Experiment details and "
                    "reingest."
                )
            )
            self._block_object("dataset", object_dictionary)
        comparison_keys = self.dataset_comparison_keys
        dataset_id = object_dictionary["description"]
        if "dataset" in self.mytardis_setup["objects_with_ids"]:
            try:
                dataset_id = object_dictionary["persistent_id"]
            except KeyError:
                logger.warning(
                    (
                        f"No persistent ID found for dataset {object_dictionary['description']}"
                    )
                )
        if self._verify_object_unblocked("dataset", dataset_id):
            match = self._match_or_block_object(
                "dataset",
                object_dictionary,
                mytardis_dictionary,
                comparison_keys,
            )
            return match
        logger.warning(
            (
                f"The dataset_id, {dataset_id} has been blocked for ingestion due to a "
                "mismatch. Please refer to the log files to identify the cause of this issue."
            )
        )
        return None

    def match_or_block_datafile(
        self,
        object_dictionary: dict,
        mytardis_dictionary: dict,
    ) -> Union[None, str]:
        """Function that compares two dictionaries using the match_object_with_dictionary
        function from the helpers module. If a perfect match can't be found then block
        the dataset_id for cascading purposes.

        Note: This function will also block an dataset on the basis of it's dataset_id
        if that ID is found in the blocked datasets. This is to ensure that we don't have the
        case in which data is appended to an incorrect experiment

        Args:
            object_dictionary: the ingestion dictionary that has matched with a hit from MyTardis
            mytardis_dictionary: the resulting dictionary from MyTardis that has matched

        Returns:
            The uri as a string if the match is perfect, otherwise None and adds the id to the
                self.blocked_ids for dataset
        """
        cleaned_dict = self._remove_blocked_parents_from_object_dict(
            "dataset",
            "datafile",
            object_dictionary,
        )
        if cleaned_dict is None:
            logger.warning(
                (
                    f"There are no valid parents for datafile {object_dictionary['filename']}. "
                    "Blocking the datafile. Please check the Dataset details and "
                    "reingest."
                )
            )
            self._block_object("datafile", object_dictionary)
        comparison_keys = self.datafile_comparison_keys
        datafile_id = object_dictionary["filename"]
        if self._verify_object_unblocked("datafile", datafile_id):
            match = self._match_or_block_object(
                "datafile",
                object_dictionary,
                mytardis_dictionary,
                comparison_keys,
            )
            return match
        logger.warning(
            (
                f"The datafile_id, {datafile_id} has been blocked for ingestion due to a "
                "mismatch. Please refer to the log files to identify the cause of this issue."
            )
        )
        return None

    def _process_project(
        self,
        raw_dictionary: dict,
    ) -> Union[str, None]:
        """Function to process a project dictionary in it's raw, parsed form from a metadata
        file or other data source.

        Args:
            raw_dictionary: the project dictionary as parsed

        Returns:
            The uri of a matching object in MyTardis, or of the newly created object, of None
                if the forging process could not be completed
        """
        object_dict, parameter_dict = self.smelter.smelt_project(raw_dictionary)
        project_name = object_dict["name"]
        potential_matches = self.get_project_from_mytardis_if_it_already_exists(
            project_name
        )
        if potential_matches is None:
            return None
        if potential_matches:
            for match in potential_matches:
                uri = self.match_or_block_project(
                    object_dict,
                    match,
                )
            return uri
        object_dict = self.crucible.prepare_project(object_dict)
        # pylint: disable=unused-variable
        (
            uri,
            object_flag,
            parameter_flag,
        ) = self.forge.forge_project(  # pylint: disable=unused-variable
            object_dict,
            parameter_dict,
        )
        # TODO log errors etc.
        return uri

    def _process_experiment(
        self,
        raw_dictionary: dict,
    ) -> Union[str, None]:
        """Function to process an experiment dictionary in it's raw, parsed form from a metadata
        file or other data source.

        Args:
            raw_dictionary: the experiment dictionary as parsed

        Returns:
            The uri of a matching object in MyTardis, or of the newly created object, of None
                if the forging process could not be completed
        """
        object_dict, parameter_dict = self.smelter.smelt_experiment(raw_dictionary)
        experiment_name = object_dict["title"]
        potential_matches = self.get_experiment_from_mytardis_if_it_already_exists(
            experiment_name
        )
        if potential_matches is None:
            return None
        if potential_matches:
            for match in potential_matches:
                uri = self.match_or_block_experiment(
                    object_dict,
                    match,
                )
            return uri
        object_dict = self.crucible.prepare_experiment(object_dict)
        # pylint: disable=unused-variable
        (
            uri,
            object_flag,
            parameter_flag,
        ) = self.forge.forge_experiment(  # pylint: disable=unused-variable
            object_dict,
            parameter_dict,
        )
        # TODO log errors etc.
        return uri

    def _process_dataset(
        self,
        raw_dictionary: dict,
    ) -> Union[str, None]:
        """Function to process a dataset dictionary in it's raw, parsed form from a metadata
        file or other data source.

        Args:
            raw_dictionary: the dataset dictionary as parsed

        Returns:
            The uri of a matching object in MyTardis, or of the newly created object, of None
                if the forging process could not be completed
        """
        object_dict, parameter_dict = self.smelter.smelt_dataset(raw_dictionary)
        dataset_name = object_dict["description"]
        potential_matches = self.get_dataset_from_mytardis_if_it_already_exists(
            dataset_name
        )
        if potential_matches is None:
            return None
        if potential_matches:
            for match in potential_matches:
                uri = self.match_or_block_dataset(
                    object_dict,
                    match,
                )
            return uri
        object_dict = self.crucible.prepare_dataset(object_dict)
        # pylint: disable=unused-variable
        (
            uri,
            object_flag,
            parameter_flag,
        ) = self.forge.forge_dataset(  # pylint: disable=unused-variable
            object_dict,
            parameter_dict,
        )
        # TODO log errors etc.
        return uri

    def _process_datafile(
        self,
        raw_dictionary: dict,
    ) -> Union[str, None]:
        """Function to process a datafile dictionary in it's raw, parsed form from a metadata
        file or other data source.

        Args:
            raw_dictionary: the project dictionary as parsed

        Returns:
            The uri of a matching object in MyTardis, or of the newly created object, of None
                if the forging process could not be completed
        """
        object_dict = self.smelter.smelt_datafile(raw_dictionary)
        datafile_name = object_dict["filename"]
        potential_matches = self.get_datafile_from_mytardis_if_it_already_exists(
            datafile_name
        )
        if potential_matches is None:
            return None
        if potential_matches:
            for match in potential_matches:
                uri = self.match_or_block_datafile(
                    object_dict,
                    match,
                )
            return uri
        object_dict = self.crucible.prepare_datafile(object_dict)
        # pylint: disable=unused-variable
        (
            uri,
            object_flag,
            parameter_flag,
        ) = self.forge.forge_datafile(  # pylint: disable=unused-variable
            object_dict,
        )
        # TODO log errors etc.
        return uri

    def _get_all_projects(self) -> list:
        projects = []
        for project_file in self.object_dictionary["project"]:
            parsed_dictionaries = self.smelter.read_file(project_file)
            for parsed_dictionary in parsed_dictionaries:
                projects.append(
                    self.smelter.get_objects_in_input_file(
                        parsed_dictionary,
                        "project",
                    )
                )
        projects = [item for sublist in projects for item in sublist]
        return projects

    def _get_all_experiments(self) -> list:
        experiments = []
        for experiment_file in self.object_dictionary["experiment"]:
            parsed_dictionaries = self.smelter.read_file(experiment_file)
            for parsed_dictionary in parsed_dictionaries:
                experiments.append(
                    self.smelter.get_objects_in_input_file(
                        parsed_dictionary,
                        "experiment",
                    )
                )
        experiments = [item for sublist in experiments for item in sublist]
        return experiments

    def _get_all_datasets(self) -> list:
        datasets = []
        for dataset_file in self.object_dictionary["dataset"]:
            parsed_dictionaries = self.smelter.read_file(dataset_file)
            for parsed_dictionary in parsed_dictionaries:
                datasets.append(
                    self.smelter.get_objects_in_input_file(
                        parsed_dictionary,
                        "dataset",
                    )
                )
        datasets = [item for sublist in datasets for item in sublist]
        return datasets

    def _get_all_datafiles(self) -> list:
        datafiles = []
        for datafile_file in self.object_dictionary["datafile"]:
            parsed_dictionaries = self.smelter.read_file(datafile_file)
            for parsed_dictionary in parsed_dictionaries:
                datafiles.append(
                    self.smelter.get_objects_in_input_file(
                        parsed_dictionary,
                        "datafile",
                    )
                )
        datafiles = [item for sublist in datafiles for item in sublist]
        return datafiles

    def process_projects(self) -> list:
        """Function to get all the projects from a directory and generate the
        objects in MyTardis"""
        projects = self._get_all_projects()
        projects_created = []
        for project in projects:
            projects_created.append(self._process_project(project))
        return projects_created

    def process_experiments(self) -> list:
        """Function to get all the experiments from a directory and generate the
        objects in MyTardis"""
        experiments = self._get_all_experiments()
        experiments_created = []
        for experiment in experiments:
            experiments_created.append(self._process_experiment(experiment))
        return experiments_created

    def process_datasets(self) -> list:
        """Function to get all the datasets from a directory and generate the
        objects in MyTardis"""
        datasets = self._get_all_datasets()
        datasets_created = []
        for dataset in datasets:
            datasets_created.append(self._process_dataset(dataset))
        return datasets_created

    def process_datafiles(self) -> list:
        """Function to get all the projects from a directory and generate the
        objects in MyTardis"""
        datafiles = self._get_all_datafiles()
        datafiles_created = []
        for datafile in datafiles:
            datafiles_created.append(self._process_datafile(datafile))
        return datafiles_created

    def process_all(self) -> dict:
        """Helper funciton to run all the ingestions in one go"""
        return_dict = {}
        return_dict["projects"] = self.process_projects()
        return_dict["experiments"] = self.process_experiments()
        return_dict["datasets"] = self.process_datasets()
        return_dict["datafiles"] = self.process_datafiles()
        return return_dict
