# pylint: disable=logging-fstring-interpolation,duplicate-code
"""Smelter base class. A class that provides functions to split raw dataclasses into
refined dataclasses suited to be passed into an instance of the Crucible class for
matching existing objects in MyTardis.

In addition to splitting the raw dataclasses up it also injects some default values
where appropriate"""
import logging
from pathlib import Path
from typing import Optional, Tuple

from pydantic import ValidationError

from src.blueprints import (
    DatafileReplica,
    ParameterSet,
    RawDatafile,
    RawDataset,
    RawExperiment,
    RawProject,
    RefinedDatafile,
    RefinedDataset,
    RefinedExperiment,
    RefinedProject,
)
from src.blueprints.common_models import Parameter
from src.helpers import (
    GeneralConfig,
    IntrospectionConfig,
    SchemaConfig,
    StorageConfig,
    check_projects_enabled_and_log_if_not,
)
from src.overseers import Overseer

logger = logging.getLogger(__name__)


class Smelter:
    """The Smelter base class to be subclassed into individual concrete classes for different
    ingestion approaches.
    Smelter classes share a number of similar processing routines, especially around dictionary
    modification and date handling.
    Attributes:
    """

    def __init__(
        self,
        general: GeneralConfig,
        default_schema: SchemaConfig,
        storage: StorageConfig,
        overseer: Overseer,  # This is a temporary fix during config processor dev
        mytardis_setup: IntrospectionConfig,
    ) -> None:
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

        if mytardis_setup:
            self.projects_enabled = mytardis_setup.projects_enabled
            self.objects_with_ids = mytardis_setup.objects_with_ids
            self.objects_with_profiles = mytardis_setup.objects_with_profiles
        self.default_schema = default_schema
        self.default_institution = general.default_institution
        self.source_dir = storage.source_directory
        self.target_dir = storage.target_directory
        self.storage_box = overseer.get_storage_box(storage.box)
        if not self.storage_box:
            raise ValueError(
                "Unable to initalise Smelter due to bad storage box"
            )

    def extract_parameters(
        self,
        raw_object: RawProject | RawExperiment | RawDataset | RawDatafile,
    ) -> Optional[ParameterSet]:
        """Extract the metadata field and the schema field from a RawObject dataclass
        and use it to create a Parameterset"""
        if not raw_object.metadata:
            return None
        parameter_list = []
        for parameter in raw_object.metadata:
            for key, value in parameter.items():
                try:
                    parameter_list.append(Parameter(name=key, value=value))
                except ValidationError:
                    logger.warning(
                        (
                            f"Unable to parse paramter {key}: {value} "
                            "into Parameter"
                        ),
                        exc_info=True,
                    )
                    continue
        if parameter_list and raw_object.object_schema:
            return ParameterSet(
                schema=raw_object.object_schema, parameters=parameter_list
            )
        return None

    def smelt_project(
        self, raw_project: RawProject
    ) -> Tuple[RefinedProject, Optional[ParameterSet]] | None:
        """Inject the schema into the project dictionary if it's not
        already present. Do the same for an institution and convert to
        a RawProject dataclass for validation."""
        if not check_projects_enabled_and_log_if_not(self):
            return None
        if not raw_project.object_schema:
            if not self.default_schema or not self.default_schema.project:
                logger.warning(
                    "Unable to find default project schema and no schema provided"
                )
                return None
            raw_project.object_schema = self.default_schema.project
        if not raw_project.institution:
            if not self.default_institution:
                logger.warning(
                    "Unable to find default institution and no institution provided"
                )
                return None
            raw_project.institution = [self.default_institution]
        if raw_project.institution:
            try:
                refined_project = RefinedProject(
                    name=raw_project.name,
                    description=raw_project.description,
                    principal_investigator=raw_project.principal_investigator,
                    created_by=raw_project.created_by,
                    url=raw_project.url,
                    users=raw_project.users,
                    groups=raw_project.groups,
                    persistent_id=raw_project.persistent_id,
                    alternate_ids=raw_project.alternate_ids,
                    institution=raw_project.institution,
                    start_time=raw_project.start_time,
                    end_time=raw_project.end_time,
                    embargo_until=raw_project.embargo_until,
                )
            except ValidationError:
                logger.warning(
                    (
                        "Malformed project dataclass produced by smelter. Project "
                        f"is {raw_project.name}"
                    ),
                    exc_info=True,
                )
                return None
            parameters = self.extract_parameters(raw_project)
            return (refined_project, parameters)
        return None

    def smelt_experiment(
        self, raw_experiment: RawExperiment
    ) -> Tuple[RefinedExperiment, Optional[ParameterSet]] | None:
        """Inject the schema into the experiment dictionary if it's not
        already present.
        Convert to a RawExperiment dataclass for validation."""
        if not raw_experiment.object_schema:
            if not self.default_schema or not self.default_schema.experiment:
                logger.warning(
                    "Unable to find default experiment schema and no schema provided"
                )
                return None
            raw_experiment.object_schema = self.default_schema.experiment
        if self.projects_enabled and not raw_experiment.projects:
            logger.warning(
                "Projects enabled in MyTardis and no projects provided to link this "
                f"experiment too. Experiment provided {raw_experiment}"
            )
            return None
        if not raw_experiment.institution_name:
            if not self.default_institution:
                logger.warning(
                    "Unable to find default institution and no institution provided"
                )
                return None
            raw_experiment.institution_name = self.default_institution
        try:
            refined_experiment = RefinedExperiment(
                title=raw_experiment.title,
                description=raw_experiment.description,
                created_by=raw_experiment.created_by,
                url=raw_experiment.url,
                locked=raw_experiment.locked,
                users=raw_experiment.users,
                groups=raw_experiment.groups,
                persistent_id=raw_experiment.persistent_id,
                alternate_ids=raw_experiment.alternate_ids,
                projects=raw_experiment.projects,
                institution_name=raw_experiment.institution_name,
                start_time=raw_experiment.start_time,
                end_time=raw_experiment.end_time,
                created_time=raw_experiment.created_time,
                update_time=raw_experiment.update_time,
                embargo_until=raw_experiment.embargo_until,
            )
        except ValidationError:
            logger.warning(
                (
                    "Malformed experiment dataclass produced by smelter. Experiment "
                    f"is {raw_experiment.title}"
                ),
                exc_info=True,
            )
            return None
        parameters = self.extract_parameters(raw_experiment)
        return (refined_experiment, parameters)

    def smelt_dataset(
        self, raw_dataset: RawDataset
    ) -> Tuple[RefinedDataset, Optional[ParameterSet]] | None:
        """Inject the schema into the dataset dictionary if it's not
        already present.
        Convert to a RefinedDataset dataclass for validation."""
        if not raw_dataset.object_schema:
            if not self.default_schema or not self.default_schema.dataset:
                logger.warning(
                    "Unable to find default dataset schema and no schema provided"
                )
                return None
            raw_dataset.object_schema = self.default_schema.dataset
        try:
            refined_dataset = RefinedDataset(
                description=raw_dataset.description,
                directory=raw_dataset.directory,
                users=raw_dataset.users,
                groups=raw_dataset.groups,
                immutable=raw_dataset.immutable,
                persistent_id=raw_dataset.persistent_id,
                alternate_ids=raw_dataset.alternate_ids,
                experiments=raw_dataset.experiments,
                instrument=raw_dataset.instrument,
                created_time=raw_dataset.created_time,
                modified_time=raw_dataset.modified_time,
            )
        except ValidationError:
            logger.warning(
                (
                    "Malformed dataset dataclass produced by smelter. Dataset "
                    f"is {raw_dataset.description}"
                ),
                exc_info=True,
            )
            return None
        parameters = self.extract_parameters(raw_dataset)
        return (refined_dataset, parameters)

    def _create_replica(
        self,
        relative_file_path: Path,
    ) -> DatafileReplica | None:
        """Create a datafile replica using the filepath and the storage
        box"""
        if not self.storage_box:
            return None
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

    def _generate_relative_file_path_for_datafile(
        self,
        filename: str,
        directory: Path,
    ) -> Path | None:
        """Construct a relative file path, relative to the storage box file path
        for the datafile being ingested."""
        if not self.storage_box:
            return None
        target_dir = self.target_dir
        return Path(target_dir / directory / filename).relative_to(
            self.storage_box.location
        )

    def smelt_datafile(  # pylint: disable=too-many-return-statements
        self, raw_datafile: RawDatafile
    ) -> RefinedDatafile | None:
        """Inject the schema into the datafile dictionary if it's not
        already present.
        Process the file path into a replica and append to the dictionary
        Convert to a RawDatafile dataclass for validation."""
        if not raw_datafile.object_schema:
            if not self.default_schema or not self.default_schema.datafile:
                logger.warning(
                    "Unable to find default datafile schema and no schema provided"
                )
                return None
            raw_datafile.object_schema = self.default_schema.dataset
        relative_file_path = self._generate_relative_file_path_for_datafile(
            raw_datafile.filename, raw_datafile.directory
        )
        if not relative_file_path:
            return None
        replicas = self._create_replica(relative_file_path)
        if not replicas:
            return None
        parameters = self.extract_parameters(raw_datafile)
        try:
            refined_datafile = RefinedDatafile(
                filename=raw_datafile.filename,
                directory=raw_datafile.directory,
                md5sum=raw_datafile.md5sum,
                mimetype=raw_datafile.mimetype,
                size=raw_datafile.size,
                users=raw_datafile.users,
                groups=raw_datafile.groups,
                dataset=raw_datafile.dataset,
                replicas=[replicas],
                parameter_sets=parameters,
            )
        except ValidationError:
            logger.warning(
                (
                    "Malformed datafile dataclass produced by smelter. Datafile "
                    f"is {raw_datafile.filename}"
                ),
                exc_info=True,
            )
            return None
        return refined_datafile

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
        replica = {
            "uri": uri.as_posix(),
            "location": location,
            "protocol": "file",
        }
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

    @abstractmethod  # pragma: no cover
    def get_file_type_for_input_files(self) -> str:  # pragma: no cover
        """Function to return a string that can be used by Path.glob() to
        get all of the input files in a directory"""

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

        return []  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def read_file(self, file_path: Path) -> tuple:  # pragma: no cover
        """Function to read in different input files and process into
        dictionaries. As there may be more than one object in a file, wrap in
        a tuple and return."""

        return tuple()  # pragma: no cover

    '''@abstractmethod  # pragma: no cover
    def rebase_file_path(self, parsed_dict: dict) -> dict:  # pragma: no cover
        """Function to fix up the file path to take into account where it is
        mounted."""
        return parsed_dict  # pragma: no cover'''

    @abstractmethod  # pragma: no cover
    def expand_datafile_entry(self, parsed_dict) -> list:  # pragma: no cover
        """Function to add the additional fields (size, checksum etc.) from files"""
        return []  # pragma: no cover

    @abstractmethod  # pragma: no cover
    def get_object_from_dictionary(self, parsed_dict: dict) -> Union[str, None]:
        """Helper function to get the object type from a parsed dictionary"""
        return None  # pragma: no cover
