# pylint: disable=logging-fstring-interpolation,pointless-string-statement
"""YAML smelter. A class that processes YAML files into dictionaries suitable to be passed to an
instance of the Forge class for creating objects in MyTardis."""

import logging
from copy import deepcopy
from pathlib import Path
from typing import Union

import yaml
from src.helpers.config import (
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
)

from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


class YAMLSmelter(Smelter):
    """This Smelter class takes a YAML file as an input and processes it into dictionaries.

    The YAMLSmelter class reads in a YAML file, identifies it's type based on the keys in the file
    and passes it to the appropriate function for deconstruction into object and parameter
    dictionaries.

    Attributes:
        projects_enabled: a boolean flag indicating whether or not to process projects
        objects_with_pids: a list of objects that have identifiers
        objects_with_profiles: a list of objects that have profiles
        OBJECT_TYPES: a dictionary of object types and the smelter functions that are used
            to process the input dictionaries.
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
            general : GeneralConfig
            Pydantic config class containing general information
            default_schema : SchemaConfig
            Pydantic config class containing information about default meta data schemas
            storage : StorageConfig
            Pydantic config class containing information about storage (box, source and target paths)
            mytardis_setup : IntrospectionConfig
            Pydantic config class containing information from the introspection API
        """
        super().__init__(general, default_schema, storage, mytardis_setup)
        self.OBJECT_TYPES = {  # pylint: disable=invalid-name
            "project_name": "project",
            "experiment_name": "experiment",
            "dataset_name": "dataset",
            "datafiles": "datafile",
        }

    def get_key_conversions(self):
        """Get the translation dictionary between the YAML key names and the MyTardis
        object names. This has been added to allow for standardisation of the naming system
        in the YAML files to make it clearer for researchers.

        Returns:
            A dictionary of conversions with the YAML key used as a key and the MyTardis key
                the value in the key: value pair
        """
        return {
            "project": {
                "project_name": "name",
                "lead_researcher": "principal_investigator",
                "project_id": "persistent_id",
            },
            "experiment": {
                "experiment_name": "title",
                "experiment_id": "persistent_id",
                "project_id": "projects",
            },
            "dataset": {
                "dataset_name": "description",
                "experiment_id": "experiments",
                "dataset_id": "persistent_id",
                "instrument_id": "instrument",
            },
            "datafile": {},
        }

    def _tidy_up_metadata_keys(  # pylint: disable=no-self-use
        self, parsed_dict: dict, object_type: str
    ) -> dict:
        """Function to get rid of spaces and convert human readable metadata keys to snakecase"""
        cleaned_dict = deepcopy(parsed_dict)
        if "metadata" in parsed_dict.keys():
            for key in parsed_dict["metadata"].keys():
                value = cleaned_dict["metadata"].pop(key)
                new_key = object_type + "_" + key.replace(" ", "_").lower()
                cleaned_dict[new_key] = value
            cleaned_dict.pop("metadata")
        return cleaned_dict

    def get_file_type_for_input_files(self) -> str:  # pylint: disable=no-self-use
        """Function to return a string that can be used by Path.glob() to
        get all of the input files in a directory"""

        return "*.yaml"

    def get_object_types_in_input_file(self, file_path: Path) -> tuple:
        """Takes a file path for a YAML file and returns a tuple of object types contained with the
        file

        Calls YAMLSmelter.read_yaml_file to read in a YAML file into a tuple of dictionaries.
        For each of the dictionaries read, adds the object types to a tuple of types and returns
        this tuple

        Args:
            file_path: a Path to the YAML file to be processed

        Returns:
            A tuple of object types within the file
        """
        object_types: list = []
        parsed_dictionaries = self.read_file(file_path)
        for parsed_dict in parsed_dictionaries:
            object_type_key = list(set(parsed_dict).intersection(self.OBJECT_TYPES))
            if len(object_type_key) == 0:
                logger.warning(
                    f"File {file_path} was not recognised as a MyTardis ingestion file"
                )
                object_types.append(None)
                continue
            if len(object_type_key) > 1:
                logger.warning(
                    (
                        f"Malformed MyTardis ingestion file, {file_path}. Please ensure that "
                        "sections are properly delimited with '---' and that each section is "
                        "defined for one object type only i.e. 'project', 'experiment', "
                        "'dataset' or 'datafile'."
                    )
                )
                object_types.append(None)
                continue
            object_types.append(self.OBJECT_TYPES[object_type_key[0]])
        return (*object_types,)

    def get_object_types_in_input_file(self, file_path: Path, object_type: str) -> list:
        """Takes a file path for a YAML file and returns a list of object of a given type
        contained within the file

        Calls YAMLSmelter.read_yaml_file to read in a YAML file into a tuple of dictionaries.

        Args:
            file_path: a Path to the YAML file to be processed

        Returns:
            A tuple of object types within the file
        """
        objects = []
        parsed_dictionaries = self.read_file(file_path)
        for parsed_dict in parsed_dictionaries:
            if self.get_object_from_dictionary(parsed_dict) == object_type:
                objects.append(parsed_dict)
        return objects

    def read_file(self, file_path: Path) -> tuple:  # pylint: disable=no-self-use
        """Takes a file path for a YAML file and reads it into a tuple of dictionaries.

        Reads in a YAML file. As it is possible for multiple YAML documents to be placed in
        a single file, the function also checks to see if a generator has been yielded and creates
        a tuple of dictionaries as a return.

        Args:
           file_path: a Path to the YAML file to be processed

        Returns:
           A tuple of dictionaries containing the parsed YAML file.
        """
        try:
            with open(file_path, "r", encoding="UTF-8") as open_file:
                parsed_dictionaries = yaml.safe_load_all(open_file.read())
        except Exception as error:
            logger.error(f"Failed to read file {file_path}.", exc_info=True)
            raise error
        return_list = []
        for dictionary in parsed_dictionaries:
            return_list.append(dictionary)
        return tuple(return_list)

    def expand_datafile_entry(  # pylint: disable=too-many-nested-blocks
        self, parsed_dict: dict
    ) -> list:
        """Function to read in a data file entry from the YAML file, locate the file and
        generate a known good MD5 sum, determine the file size and create a file replica
        ready for ingestion into MyTardis

        Args:
            parsed_dict: a dictionary containing the data file entries from the YAML file

        Returns:
            a list of dictionaries ready for ingestion into MyTardis
        """
        file_list = []
        for file_dict in parsed_dict["datafiles"]["files"]:
            try:
                file_dict["file_path"] = Path(file_dict["file_path"])
            except KeyError:
                logger.warning(
                    (
                        "Parsed datafile dictionary is missing a 'file_path' field. "
                        f"The dictionary passed in was {file_dict}."
                    )
                )
                continue
            if not file_dict["file_path"].is_file():
                for filename in file_dict["file_path"].iterdir():
                    if filename.is_file():
                        cleaned_dict = {}
                        cleaned_dict["dataset"] = parsed_dict["datafiles"][
                            "dataset_id"
                        ][0]
                        if "metadata" in file_dict.keys():
                            for key in file_dict["metadata"].keys():
                                cleaned_dict[key] = file_dict["metadata"][key]
                        cleaned_dict["filename"] = Smelter.get_filename_from_filepath(
                            filename
                        )
                        cleaned_dict["md5sum"] = Smelter.get_file_checksum(filename)
                        cleaned_dict["size"] = filename.stat().st_size
                        cleaned_dict["file_path"] = Path(filename).relative_to(
                            self.source_dir
                        )
                        cleaned_dict = self._create_replica(cleaned_dict)
                        file_list.append(cleaned_dict)
            else:
                cleaned_dict = {}
                cleaned_dict["dataset"] = parsed_dict["datafiles"]["dataset_id"][0]
                if "metadata" in file_dict.keys():
                    for key in file_dict["metadata"].keys():
                        cleaned_dict[key] = file_dict["metadata"][key]
                cleaned_dict["filename"] = Smelter.get_filename_from_filepath(
                    file_dict["file_path"]
                )
                cleaned_dict["md5sum"] = Smelter.get_file_checksum(
                    file_dict["file_path"]
                )
                cleaned_dict["size"] = file_dict["file_path"].stat().st_size
                cleaned_dict["file_path"] = Path(file_dict["file_path"]).relative_to(
                    self.source_dir
                )
                cleaned_dict = self._create_replica(cleaned_dict)
                file_list.append(cleaned_dict)
        return file_list

    def get_object_from_dictionary(  # pylint: disable=consider-using-dict-items,consider-iterating-dictionary
        self, parsed_dict: dict
    ) -> Union[str, None]:
        """Helper function to get the object type from a parsed dictionary"""
        for key in self.OBJECT_TYPES.keys():
            if key in parsed_dict.keys():
                return self.OBJECT_TYPES[key]
        return None
