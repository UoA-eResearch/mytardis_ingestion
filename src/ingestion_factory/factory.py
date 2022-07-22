# pylint: disable=consider-using-set-comprehension
"""IngestionFactory is a base class for specific instances of MyTardis
Ingestion scripts. The base class contains mostly concrete functions but
needs to determine the Smelter class that is used by the Factory"""

from pathlib import Path
from typing import Union

from src.forges import Forge
from src.helpers.config import (
    AuthConfig,
    ConnectionConfig,
    GeneralConfig,
    IntrospectionConfig,
)
from src.overseers import Overseer
from src.smelters import Smelter


class IngestionFactory:
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
        self.glob_string: For smelters that use files, what is the extension or similar to search
            for. See pathlib documentations for glob details.
        self.default_institution: Either a name, or an identifier for an Institution to use as the
            default for Project and Experiment creation.
    """

    def __init__(
        self,
        general: GeneralConfig,
        auth: AuthConfig,
        connection: ConnectionConfig,
        mytardis_setup: IntrospectionConfig,
        smelter: Smelter,
    ) -> None:
        """Initialises the Factory with the configuration found in the config_dict.

        Passes the config_dict to the overseer and forge instances to ensure that the
        specific MyTardis configuration is shared across all classes

        Args:
            general : GeneralConfig
            Pydantic config class containing general information
            auth : AuthConfig
            Pydantic config class containing information about authenticating with a MyTardis instance
            connection : ConnectionConfig
            Pydantic config class containing information about connecting to a MyTardis instance
            mytardis_setup : IntrospectionConfig
            Pydantic config class containing information from the introspection API
            smelter : Smelter
            class instance of Smelter
        """
        self.overseer = Overseer(auth, connection, mytardis_setup)
        self.mytardis_setup = mytardis_setup
        self.forge = Forge(auth, connection)
        self.smelter = smelter
        self.glob_string = self.smelter.get_file_type_for_input_files()
        self.default_institution = general.default_institution

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
            if not Overseer.is_uri(cleaned_dict[key_name], object_type):
                if (
                    self.mytardis_setup.objects_with_ids
                    and object_type in self.mytardis_setup.objects_with_ids
                ):
                    objects = self.overseer.get_uris_by_identifier(
                        object_type, cleaned_dict[key_name]
                    )
                    if (
                        object_type not in self.mytardis_setup.objects_with_ids
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
        if (
            self.mytardis_setup.objects_with_ids
            and "project" in self.mytardis_setup.objects_with_ids
        ):
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
        if (
            self.mytardis_setup.objects_with_ids
            and "experiment" in self.mytardis_setup.objects_with_ids
        ):
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
        if (
            self.mytardis_setup.objects_with_ids
            and "dataset" in self.mytardis_setup.objects_with_ids
        ):
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
        if (
            self.mytardis_setup.objects_with_ids
            and "instrument" in self.mytardis_setup.objects_with_ids
        ):
            uri = self.overseer.get_uris_by_identifier("instrument", instrument)
        if not uri:
            uri = self.overseer.get_uris("instrument", "name", instrument)
        return uri

    def process_projects(self, file_path: Path) -> list:
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
                    object_dict = self.replace_search_term_with_uri(
                        "institution", object_dict, "name"
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

    def process_experiments(  # pylint: disable=too-many-locals
        self, file_path: Path
    ) -> list:
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
        datafile_files = self.build_object_lists(file_path, "datafile")
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
