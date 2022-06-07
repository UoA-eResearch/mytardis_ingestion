# pylint: disable=consider-using-set-comprehension,logging-fstring-interpolation,unnecessary-comprehension
"""IngestionFactory is a base class for specific instances of MyTardis
Ingestion scripts. The base class contains mostly concrete functions but
needs to determine the Smelter class that is used by the Factory"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Union

from src.forges import Forge
from src.helpers import is_uri
from src.overseers import Overseer

logger = logging.getLogger(__name__)


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
            set up.
        self.forge: An instance of the Forge class
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
        self.mytardis_setup = self.overseer.get_mytardis_set_up()
        config_dict.update(self.mytardis_setup)
        self.forge = Forge(config_dict)
        self.smelter = self.get_smelter(config_dict)
        self.default_institution = config_dict["default_institution"]
        self.input_file_list: list = []

    @abstractmethod  # pragma: no cover
    def get_smelter(self, config_dict: dict):  # pragma: no cover
        """Abstract method to return the specific instance of a smelter for the
        concrete instance of the IngestionFactory.

        Returns:
            None
        """
        return None  # pragma: no cover

    def build_object_dict(
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
            objects = self.smelter.get_objects_in_input_file(file_path)
            objects = [object_types for object_types in objects]
            for object_type in objects:
                return_dict[object_type].append(file_path)
        return return_dict

    def replace_search_term_with_uri(
        self,
        object_type: str,
        cleaned_dict: dict,
        fallback_search: str,
        key_name: str = None,
    ) -> dict:
        """Helper function to carry out a search using an identifier or name
        and replace it's value with a URI from MyTardis

        Args:
            object_type: A string representation of the object type in MyTardis to search for
            cleaned_dict: A dictionary containing the key to be replaced
            fallback_search: The name of the search key should searching by identifier fail
            key_name: The name of the key to replace, if it is not the object_type.

        Returns:
            The cleaned_dict dictionary with the search term replaced by a URI from MyTardis
        """
        if not key_name:
            key_name = object_type
        objects: Union[list, None] = []
        if key_name in cleaned_dict.keys():
            if not is_uri(cleaned_dict[key_name], object_type):
                if object_type in self.mytardis_setup["objects_with_ids"]:
                    objects = self.overseer.get_uris_by_identifier(
                        object_type, cleaned_dict[key_name]
                    )
                    if (
                        object_type not in self.mytardis_setup["objects_with_ids"]
                        or not objects
                    ):
                        objects = self.overseer.get_uris(
                            object_type, fallback_search, cleaned_dict[key_name]
                        )
                    cleaned_dict[key_name] = objects
        return cleaned_dict

    def get_project_uri(self, project_id):
        """Helper function to get a Project URI from MyTardis

        Args:
            project_id: An identifier or project name to search for

        Returns:
            The URI from MyTardis for the project searched for.
        """
        uri = None
        if "project" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("project", project_id)
        if not uri:
            uri = self.overseer.get_uris("project", "name", project_id)
        return uri

    def get_experiment_uri(self, experiment_id):
        """Helper function to get an Experiment URI from MyTardis

        Args:
            experiment_id: An identifier or experiment name to search for

        Returns:
            The URI from MyTardis for the experiment searched for.
        """
        uri = None
        if "experiment" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("experiment", experiment_id)
        if not uri:
            uri = self.overseer.get_uris("experiment", "title", experiment_id)
        return uri

    def get_dataset_uri(self, dataset_id):
        """Helper function to get a Dataset URI from MyTardis

        Args:
            dataset_id: An identifier or dataset name to search for

        Returns:
            The URI from MyTardis for the dataset searched for.
        """
        uri = None
        if "dataset" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("dataset", dataset_id)
        if not uri:
            uri = self.overseer.get_uris("dataset", "description", dataset_id)
        return uri

    def get_instrument_uri(self, instrument):
        """Helper function to get a Instrumentt URI from MyTardis

        Args:
            instrument_id: An identifier or instrument name to search for

        Returns:
            The URI from MyTardis for the instrument searched for.
        """
        uri = None
        if "instrument" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("instrument", instrument)
        if not uri:
            uri = self.overseer.get_uris("instrument", "name", instrument)
        return uri

    def compare_object_from_mytardis_with_object_dictionary():
        pass

    @staticmethod
    def invalid_object_passed_to_process_object(parsed_dict: dict) -> tuple:
        """Handler to create logger warning and return a non-breaking return
        in case a non-MyTardis object is passed to process_object. NB: This should
        never be called.

        Args:
            parsed_dict: the dictionary parsed from the smelter.read_file function

        Returns:
            A tuple of (None, None) representing the inability of the processing
                script to handle the invalid object
        """
        logger.warning(
            (
                "IngestionFactory._process_object was unable to process the parsed "
                "dictionary due to an invalid object key being extracted from the "
                f"dictionary. Input dictionary was: {parsed_dict}."
            )
        )
        return (None, None)

    def _process_object(self, parsed_dict: dict) -> tuple:
        """Placeholder for now"""
        object_type = self.smelter.get_object_from_dictionary(parsed_dict)
        if object_type is None:
            logger.warning(
                "The dictionary passed to be processed is not recognised as a "
                "MyTardis input dictionary\nThe dictionary in question is: "
                f"{parsed_dict}"
            )
            return (None, None)
        smelter_functions = {
            "project": self.smelter.smelt_project,
            "experiment": self.smelter.smelt_experiment,
            "dataset": self.smelter.smelt_dataset,
            "datafile": self.smelter.smelt_datafile,
        }
        print(smelter_functions)
        return (None, None)

    def process_projects(self, file_path: Path) -> list:
        """Wrapper function to create the projects from input files"""
        project_files = self.build_object_lists(file_path, "project")  # refactor
        return_list = []
        for project_file in project_files:
            parsed_dictionaries = self.smelter.read_file(project_file)
            for parsed_dict in parsed_dictionaries:
                object_type = self.smelter.get_object_from_dictionary(parsed_dict)
                if object_type == "project":
                    object_dict, parameter_dict = self.smelter.smelt_project(
                        parsed_dict
                    )
                    object_dict = self.replace_search_term_with_uri(
                        "institution", object_dict, "name"
                    )
                    forged_object = self.forge.forge_object(
                        object_dict["name"], "project", object_dict
                    )
                    return_list.append(forged_object)
                    uri = None
                    if forged_object[1]:
                        uri = forged_object[2]
                    if parameter_dict["parameters"] != [] and uri:
                        parameter_dict["project"] = uri
                        return_list.append(
                            self.forge.forge_object(
                                f"{object_dict['name']} - Parameters",
                                "projectparameterset",
                                parameter_dict,
                            )
                        )
        return return_list

    def process_experiments(  # pylint: disable=too-many-locals
        self, file_path: Path
    ) -> list:
        """Wrapper function to create the experiments from input files"""
        experiment_files = self.build_object_lists(file_path, "experiment")  # refactor
        return_list = []
        for experiment_file in experiment_files:
            parsed_dictionaries = self.smelter.read_file(experiment_file)
            for parsed_dict in parsed_dictionaries:
                object_type = self.smelter.get_object_from_dictionary(parsed_dict)
                if object_type == "experiment":
                    object_dict, parameter_dict = self.smelter.smelt_experiment(
                        parsed_dict
                    )
                    name = object_dict["title"]
                    project_id = object_dict["projects"]
                    if isinstance(project_id, str):
                        project_id = [project_id]
                    project_uris = []
                    for project in project_id:
                        project_uris.append(self.get_project_uri(project))
                    project_uris = list(
                        set([item for sublist in project_uris for item in sublist])
                    )
                    object_dict["projects"] = project_uris
                    forged_object = self.forge.forge_object(
                        name, "experiment", object_dict
                    )
                    return_list.append(forged_object)
                    uri = None
                    if forged_object[1]:
                        uri = forged_object[2]
                    if parameter_dict["parameters"] != [] and uri:
                        parameter_dict["experiment"] = uri
                        return_list.append(
                            self.forge.forge_object(
                                f"{name} - Parameters",
                                "experimentparameterset",
                                parameter_dict,
                            )
                        )
        return return_list

    def process_datasets(  # pylint: disable=too-many-locals
        self, file_path: Path
    ) -> list:
        """Wrapper function to create the experiments from input files"""
        dataset_files = self.build_object_lists(file_path, "dataset")  # refactor
        return_list = []
        for dataset_file in dataset_files:
            parsed_dictionaries = self.smelter.read_file(dataset_file)
            for parsed_dict in parsed_dictionaries:
                object_type = self.smelter.get_object_from_dictionary(parsed_dict)
                if object_type == "dataset":
                    object_dict, parameter_dict = self.smelter.smelt_dataset(
                        parsed_dict
                    )
                    name = object_dict["description"]
                    experiments = object_dict["experiments"]
                    if isinstance(experiments, str):
                        experiments = [experiments]
                    experiment_uris = []
                    for experiment in experiments:
                        experiment_uris.append(self.get_experiment_uri(experiment))
                    experiment_uris = list(
                        set([item for sublist in experiment_uris for item in sublist])
                    )
                    object_dict["experiments"] = experiment_uris
                    instrument = object_dict["instrument"]
                    object_dict["instrument"] = self.get_instrument_uri(instrument)[0]
                    forged_object = self.forge.forge_object(
                        name, "dataset", object_dict
                    )
                    return_list.append(forged_object)
                    uri = None
                    if forged_object[1]:
                        uri = forged_object[2]
                    if parameter_dict["parameters"] != [] and uri:
                        parameter_dict["dataset"] = uri
                        return_list.append(
                            self.forge.forge_object(
                                f"{name} - Parameters",
                                "datasetparameterset",
                                parameter_dict,
                            )
                        )
        return return_list

    def process_datafiles(self, file_path: Path) -> list:
        """Wrapper function to create the experiments from input files"""
        datafile_files = self.build_object_lists(file_path, "datafile")  # refactor
        return_list = []
        for datafile_file in datafile_files:
            parsed_dictionaries = self.smelter.read_file(datafile_file)
            for parsed_dict in parsed_dictionaries:
                object_type = self.smelter.get_object_from_dictionary(parsed_dict)
                if object_type == "datafile":
                    parsed_dict["datafiles"]["dataset_id"] = self.get_dataset_uri(
                        parsed_dict["datafiles"]["dataset_id"]
                    )
                    cleaned_list = self.smelter.expand_datafile_entry(parsed_dict)
                    for cleaned_dict in cleaned_list:
                        name = cleaned_dict["filename"]
                        object_dict, parameter_dict = self.smelter.smelt_datafile(
                            cleaned_dict
                        )
                        forged_object = self.forge.forge_object(
                            name, "dataset_file", object_dict
                        )
                        return_list.append(forged_object)
                        uri = None
                        if forged_object[1]:
                            uri = forged_object[2]
                        if parameter_dict["parameters"] != [] and uri:
                            parameter_dict["dataset"] = uri
                            return_list.append(
                                self.forge.forge_object(
                                    f"{name} - Parameters",
                                    "datafileparameterset",
                                    parameter_dict,
                                )
                            )
        return return_list
