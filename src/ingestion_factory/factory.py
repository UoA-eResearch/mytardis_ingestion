from abc import ABC, abstractmethod, abstractstaticmethod
from pathlib import Path
from typing import Union

import pysnooper

from src.forges import Forge
from src.overseers import Overseer


class IngestionFactory(ABC):
    """Ingestion Factory base class to orchestrate the Smelting, and Forging of
    objects within MyTardis.

    IngestionFactory is an abstract base class from which specific ingestion
    factories should be subclassed. The Factory classes orchestrate the various
    classes associated with ingestion in such a way that the Smelter, Overseer and Forge
    classes are unaware of each other

    Attributes:
        self.overseer: An instance of the Overseer class
        self.forge: An instance of the Forge class
        self.smelter: An instance of a smelter class that varies for different ingestion approaches
        self.project_factory: An instance of the ProjectFactory inner class
        self.experiment_factory: A Path to the directory holding the experiment input files
        self.dataset_factory: A Path to the directory holding the dataset input files
        self.datafile_factory: A Path to the directory holding the datafile input files.
            NB: This is NOT necessarily the same directory as the data files themselves - this
            should be defined in the input files
    """

    def __init__(self, config_dict: dict) -> None:
        """Initialises the Factory with the configuration found in the config_dict.

        Passes the config_dict to the overseer and forge instances to ensure that the
        specific MyTardis configuration is shared across all classes

        Args:
           config_dict: A configuration dictionary containing the keys required to
                initialise a MyTardisRESTFactory instance.
        """
        self.overseer = Overseer(config_dict)
        self.mytardis_setup = self.overseer.get_mytardis_set_up()
        config_dict.update(self.mytardis_setup)
        self.forge = Forge(config_dict)
        self.smelter = self.get_smelter(config_dict)
        self.glob_string = self.smelter.get_file_type_for_input_files()
        self.default_institution = config_dict["default_institution"]

    @abstractmethod
    def get_smelter(self, config_dict: dict):
        """Abstract method to return the specific instance of a smelter for the
        concrete instance of the IngestionFactory.

        Returns:
            None
        """
        return None

    def build_object_lists(self, file_path: Path, object_type: str) -> list:
        """General function to glob for files and return a list with the files that
        have objects of object_type defined within them

        Args:
            file_path: A Path object, ideally to the directory containing the input files
               but which can also be a parent directory or a single file.
            object_type: The object that is being sought.

        Returns:
            A list of files that have inputs of type object_type
        """
        return_list = []
        if file_path.is_file():
            if object_type in self.smelter.get_objects_in_input_file(file_path):
                return [file_path]
            return []
        for input_file in file_path.rglob(self.glob_string):
            if object_type in self.smelter.get_objects_in_input_file(input_file):
                return_list.append(input_file)
        return return_list

    @staticmethod
    def insert_default_schema_into_cleaned_dict(
        cleaned_dict: dict, default_schema: str
    ) -> dict:
        """Helper function for cases where there is only one type of schema in use for an
        object and thus it can be defined externally and not be included in the input files"""
        cleaned_dict["schema"] = default_schema
        return cleaned_dict

    @staticmethod
    def refine_smelted_object_dict(cleaned_dict: dict, updates: dict) -> None:
        """Helper function to add or replace key value pairs in a dictionary"""
        return cleaned_dict.update(updates)

    def replace_search_term_with_uri(
        self,
        object_type: str,
        cleaned_dict: dict,
        fallback_search: str,
        key_name: str = None,
    ) -> dict:
        if not key_name:
            key_name = object_type
        objects = []
        if key_name in cleaned_dict.keys():
            if not Overseer.is_uri(cleaned_dict[key_name], object_type):
                if object_type in self.objects_with_ids:
                    objects = self.overseer.get_uris_by_identifier(
                        object_type, cleaned_dict[key_name]
                    )
                    if object_type not in self.objects_with_ids or not objects:
                        objects = self.overseer.get_uris(
                            object_type, fallback_search, cleaned_dict[key_name]
                        )
        cleaned_dict[key_name] = objects
        return cleaned_dict

    def get_institution_uri(self, institution):
        uri = None
        if "institution" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("institution", institution)
        if not uri:
            uri = self.overseer.get_uris("institution", "name", institution)
        return uri

    def get_project_uri(self, project_id):
        uri = None
        if "project" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("project", project_id)
        if not uri:
            uri = self.overseer.get_uris("project", "name", project_id)
        return uri

    def get_experimentt_uri(self, experiment_id):
        uri = None
        if "experiment" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("experiment", experiment_id)
        if not uri:
            uri = self.overseer.get_uris("experiment", "title", experiment_id)
        return uri

    def get_dataset_uri(self, dataset_id):
        uri = None
        if "dataset" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("dataset", dataset_id)
        if not uri:
            uri = self.overseer.get_uris("dataset", "description", dataset_id)
        return uri

    def get_instrument_uri(self, instrument):
        uri = None
        if "instrument" in self.mytardis_setup["objects_with_ids"]:
            uri = self.overseer.get_uris_by_identifier("instrument", instrument)
        if not uri:
            uri = self.overseer.get_uris("instrument", "name", instrument)
        return uri

    @staticmethod
    def set_schema_to_default_if_not_defined(
        cleaned_dict: dict, default_schema: str
    ) -> Union[dict, None]:
        if "schema" not in cleaned_dict.keys():
            if default_schema:
                cleaned_dict = IngestionFactory.insert_default_schema_into_cleaned_dict(
                    cleaned_dict, default_schema
                )
                return cleaned_dict
        return None

    def process_projects(self, file_path: Path, default_schema: str = None) -> list:
        """Wrapper function to create the projects from input files"""
        project_files = self.build_object_lists(file_path, "project")
        return_list = []
        for project_file in project_files:
            parsed_dictionaries = self.smelter.read_file(project_file)
            for parsed_dict in parsed_dictionaries:
                object_type = self.smelter.get_object_from_dictionary(parsed_dict)
                if object_type == "project":
                    object_dict, parameter_dict = self.smelter.smelt_project(
                        parsed_dict
                    )
                    name = object_dict["name"]
                    institution_id = object_dict["institution"]
                    object_dict["institution"] = self.get_institution_uri(
                        institution_id
                    )
                    forged_object = self.forge.forge_object(
                        name, "project", object_dict
                    )
                    return_list.append(forged_object)
                    uri = None
                    if forged_object[1]:
                        uri = forged_object[2]
                    if parameter_dict["parameters"] != [] and uri:
                        parameter_dict["project"] = uri
                        return_list.append(
                            self.forge.forge_object(
                                f"{name} - Parameters",
                                "projectparameterset",
                                parameter_dict,
                            )
                        )
        return return_list

    @pysnooper.snoop()
    def process_experiments(self, file_path: Path, default_schema: str = None) -> list:
        """Wrapper function to create the experiments from input files"""
        experiment_files = self.build_object_lists(file_path, "experiment")
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
                    project_uris = []
                    for project in project_id:
                        project_uris.append(self.get_project_uri(project))
                    project_uris = [
                        item for sublist in project_uris for item in sublist
                    ]
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

    @pysnooper.snoop()
    def process_datasets(self, file_path: Path, default_schema: str = None) -> list:
        """Wrapper function to create the experiments from input files"""
        dataset_files = self.build_object_lists(file_path, "dataset")
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
                    experiment_uris = []
                    for experiment in experiments:
                        experiment_uris.append(self.get_experimentt_uri(experiment))
                    experiment_uris = [
                        item for sublist in experiment_uris for item in sublist
                    ]
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

    def process_datafiles(self, file_path: Path, default_schema: str = None) -> list:
        """Wrapper function to create the experiments from input files"""
        datafile_files = self.build_object_lists(file_path, "datafile")
        return_list = []
        for datafile_file in datafile_files:
            parsed_dictionaries = self.smelter.read_file(datafile_file)
            for parsed_dict in parsed_dictionaries:
                object_type = self.smelter.get_object_from_dictionary(parsed_dict)
                if object_type == "datafile":
                    cleaned_dict = self.smelter.rebase_file_path(parsed_dict)
                    print(cleaned_dict)
                    cleaned_dict["datafiles"]["dataset_id"] = self.get_dataset_uri(
                        cleaned_dict["datafiles"]["dataset_id"]
                    )
                    cleaned_list = self.smelter.expand_datafile_entry(cleaned_dict)
                    for cleaned_dict in cleaned_list:
                        print(cleaned_dict)
                        name = cleaned_dict["filename"]
                        object_dict, parameter_dict = self.smelter.smelt_datafile(
                            cleaned_dict
                        )
                        print(object_dict)
                        print(parameter_dict)
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
                                self.forge_object(
                                    f"{name} - Parameters",
                                    "datafileparameterset",
                                    parameter_dict,
                                )
                            )
        return return_list
