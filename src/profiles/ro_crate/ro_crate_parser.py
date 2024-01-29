"""
Classes for handling an RO-crate input into ingestible data objects
"""
# pylint: disable=missing-function-docstring

import json

# ---Imports
import logging
import mimetypes
from datetime import datetime
from pathlib import Path
from typing import Any

from rocrate.model.data_entity import DataEntity
from rocrate.rocrate import ROCrate
from rocrate.utils import get_norm_value

from src.blueprints.common_models import GroupACL, UserACL
from src.blueprints.custom_data_types import validate_isodatetime, validate_url
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import SchemaConfig
from src.extraction.ingestibles import IngestibleDataclasses
from src.mytardis_client.enumerators import MyTardisObject
from src.profiles.ro_crate.crate_to_tardis_mapper import CrateToTardisMapper
from src.utils.filesystem import checksums, filters
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.filesystem.filters import PathFilterSet

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # set the level for which this logger will be printed.


def handle_datetime(time_info: str) -> datetime:
    time_info = validate_isodatetime(time_info)
    return datetime.fromisoformat(time_info)


class ROCrateParser:
    """Parses JSON and file strucures in an RO-Crate into ingestible MyTardis dataclasses"""

    def __init__(
        self,
        crate_root_path: Path,
        crate_to_tardis_schema: Path,
        schema_config: SchemaConfig,
        crate_name: str = "",
    ) -> None:
        """Initalizes the ROCrate object and the lookup table for mapping by reading them from disk

        Args:
            crate_root_path (Path): Path to the RO-crate object
            crate_to_tardis_schema (Path): Path to json file containing mapping for schema
        """
        self.schema_config = schema_config
        with open(crate_to_tardis_schema, encoding="utf-8") as f:
            self.mapper: CrateToTardisMapper = CrateToTardisMapper(json.load(f))
        self.crate = ROCrate(Path(crate_root_path))
        self.crate_name = crate_name
        if not self.crate_name:
            # To be replaced with UUID defined in RO-Crate root
            self.crate_name = self.crate.source.parts[-2]

        self.filters = PathFilterSet(True)

    def _apply_crate_name(self, entity_id: str) -> str:
        return str(self.crate_name + "/" + entity_id)

    def _read_metadata(
        self, metadata_list: list[str]
    ) -> dict[str, str | int | float | bool]:
        metadatadict = {}
        for meta_data_id in metadata_list:
            if metadata_element := self.crate.dereference(meta_data_id):
                if md_name := metadata_element.get("name"):
                    metadatadict[md_name] = metadata_element.get("value")
        return metadatadict

    def _acl_from_person(self, user_id: str) -> UserACL:
        """A helper function to derefence an ROCrate id to produce a userACL
        Args:
            user_id (str): ROCrate User ID

        Returns:
            UserACL: an Access level control object based on person in the ROCrate
        """
        username = user_id
        crate_user = self.crate.dereference(user_id)
        if crate_user:
            username = crate_user["name"]
        user_acl: UserACL = UserACL(user=username)
        return user_acl

    def _acl_from_group(self, group_id: str) -> GroupACL:
        """A helper function to derefence an ROCrate id to produce a groupACL
        Args:
            Group_id (str): ROCrate Group ID

        Returns:
            GroupACL: an Access level control object based on Group in the ROCrate
        """
        groupname = group_id
        crate_group: DataEntity = self.crate.dereference(group_id)
        if crate_group:
            groupname = crate_group["name"]
        group_acl: GroupACL = GroupACL(group=groupname)
        return group_acl

    def _process_datafile(
        self,
        filename: Path,
        parent_dataset_description: str,
        rocrate_entity: DataEntity = None,
    ) -> RawDatafile:
        """Process a datafile from a file on disk and load into a set of ingestible dataclasses

        Args:
            filename (Path): the path of the file on disk (defined as relative to crate root)
            parent_dataset (str): the URI of the dataset parent of this file
            ingestible_classes (IngestibleDataclasses): ingestible dataclasses to load the file into
            rocrate_entity (DataEntity, optional): optional, RO-crate file object. Defaults to None.

        Returns:
            IngestibleDataclasses: the ingestible dataclasses now updated with the datafile
        """
        datafile_dict: dict[str, Any] = {}
        filepath = self.crate.source / filename
        datafile_dict["filename"] = filename.as_posix()
        datafile_dict["directory"] = filepath.relative_to(self.crate.source).as_posix()
        datafile_dict["md5sum"] = checksums.calculate_md5(filepath)
        mtype, _ = mimetypes.guess_type(filepath)
        if not mtype:
            mtype = str(filepath).rsplit(".", maxsplit=1)[-1]
        datafile_dict["mimetype"] = mtype
        datafile_dict["size"] = str(filepath.stat().st_size)
        if rocrate_entity:
            rocrate_dict: dict[str, Any] = {
                (self.mapper.get_mt_field(MyTardisObject.DATAFILE, key) or key): value
                for key, value in rocrate_entity.as_jsonld().items()
            }
            datafile_dict.update(rocrate_dict)
            if datafile_meta := datafile_dict.get("metadata"):
                datafile_dict["metadata"] = self._read_metadata([datafile_meta])
        datafile_dict["dataset"] = parent_dataset_description
        print(datafile_dict["filename"], "my parent is,", datafile_dict["dataset"])
        raw_datafile: RawDatafile = RawDatafile.model_validate(datafile_dict)
        raw_datafile.object_schema = self.schema_config.datafile
        return raw_datafile

    def _process_dataset(self, crate_dataset: DataEntity) -> RawDataset:
        """Load a dataset and all of it's dataset and datafile children from an RO-Crate spec.

        Recursively load any datasets found as children of this dataset
        Then parse all datafiles found on disk not already found as part of this or another dataset.

        Args:
            crate_dataset (DataEntity): roCrate data entity representing this dataset
            ingestible_classes (IngestibleDataclasses): mytardis dataclasses for datafiles/sets

        Raises:
            ValueError: (to be updated to more apropriate error type)
            raised if the roCrate doesn't link this dataset to an experiment/data catalog

        Returns:
            IngestibleDataclasses:
            the dataclasses object updated with this dataset and all of it's datafile children
        """
        dataset_dict = {
            (self.mapper.get_mt_field(MyTardisObject.DATASET, key) or key): value
            for key, value in crate_dataset.as_jsonld().items()
        }

        dataset_dict["experiments"] = get_norm_value(
            crate_dataset.as_jsonld(), "includedInDataCatalog"
        )
        if len(dataset_dict["experiments"]) == 0:
            raise ValueError(
                "Dataset "
                + str(crate_dataset.id)
                + " not included in a DataCatalog, cannot link this dataset to experiments"
            )
        dataset_dict["description"] = self._apply_crate_name(crate_dataset.id)
        dataset_dict["directory"] = Path(crate_dataset.id)
        if dataset_dict.get("created_time"):
            dataset_dict["created_time"] = handle_datetime(dataset_dict["created_time"])
        if dataset_dict.get("modified_time"):
            dataset_dict["modified_time"] = handle_datetime(
                dataset_dict["modified_time"]
            )
        if dataset_meta := dataset_dict.get("metadata"):
            dataset_dict["metadata"] = self._read_metadata(dataset_meta)
        raw_dataset: RawDataset = RawDataset.model_validate(dataset_dict)
        raw_dataset.object_schema = self.schema_config.dataset
        return raw_dataset

    def _process_project(self, crate_project: DataEntity) -> RawProject:
        """Reads a project from RO-Crate data entity into an ingestible MyTardis Project

        Args:
            crate_project (DataEntity): The project specified in the RO-Crate object

        Returns:
            RawProject: The MyTardis ingestible project
        """
        project_dict = {
            (self.mapper.get_mt_field(MyTardisObject.PROJECT, key) or key): value
            for key, value in crate_project.as_jsonld().items()
        }
        project_dict["identifiers"] = get_norm_value(
            crate_project.as_jsonld(), "identifier"
        )
        project_dict["users"] = [
            self._acl_from_person(person_id)
            for person_id in get_norm_value(crate_project.as_jsonld(), "users")
        ]
        if project_meta := project_dict.get("metadata"):
            project_dict["metadata"] = self._read_metadata(project_meta)
        if project_dict.get("created_time"):
            project_dict["created_time"] = handle_datetime(project_dict["created_time"])
        if project_dict.get("modified_time"):
            project_dict["modified_time"] = handle_datetime(
                project_dict["modified_time"]
            )
        raw_project: RawProject = RawProject.model_validate(project_dict)
        raw_project.object_schema = self.schema_config.project
        return raw_project

    def _process_experiment(self, crate_catalog: DataEntity) -> RawExperiment:
        """Read RO-Crate data-catalogs into the equivalent experiments in MyTardis

        Args:
            crate_catalog (DataEntity): An RO-Crate entity that represents a MyTardis experiment
            (usualy data catalog)

        Returns:
            RawExperiment: The resulting raw ingestible experiment
        """
        experiment_dict = {
            (self.mapper.get_mt_field(MyTardisObject.EXPERIMENT, key) or key): value
            for key, value in crate_catalog.as_jsonld().items()
        }
        experiment_dict["identifiers"] = get_norm_value(
            crate_catalog.as_jsonld(), "identifier"
        )
        experiment_dict["users"] = [
            self._acl_from_person(person_id)
            for person_id in get_norm_value(crate_catalog.as_jsonld(), "users")
        ]
        experiment_dict["projects"] = get_norm_value(
            crate_catalog.as_jsonld(), "project"
        )
        if experiment_meta := experiment_dict.get("metadata"):
            experiment_dict["metadata"] = self._read_metadata(experiment_meta)
        raw_experiment: RawExperiment = RawExperiment.model_validate(experiment_dict)
        raw_experiment.object_schema = self.schema_config.experiment
        return raw_experiment

    def process_projects(
        self, ingestible_classes: IngestibleDataclasses
    ) -> IngestibleDataclasses:
        ingestible_classes.add_projects(
            [
                self._process_project(p)
                for p in self.crate.get_entities()
                if "Project" in p.type
            ]
        )
        return ingestible_classes

    def process_experiments(
        self, ingestible_classes: IngestibleDataclasses
    ) -> IngestibleDataclasses:
        ingestible_classes.add_experiments(
            [
                self._process_experiment(e)
                for e in self.crate.get_entities()
                if "DataCatalog" in e.type
            ]
        )
        return ingestible_classes

    def _collect_unlisted_datafiles(
        self,
        raw_datasets: list[RawDataset],
        raw_datafiles: list[RawDatafile],
        file_filter: filters.PathFilterSet,
    ) -> list[RawDatafile]:
        """Read datafiles on disk that are not explicitly listed in the RO-Crate

        Args:
            raw_datasets (list[RawDataset]): datasets already parsed from RO-Crate Json,
            used to determine parents of files based on directory location
            raw_datafiles (list[RawDatafile]): datafiles already read into the RO-crate
            file_filter (filters.PathFilterSet): Filter for specific files (i.e. system files)

        Returns:
            list[RawDatafile]: list of datafiles now updated with datfiles on disk
        """
        for traversed_dataset in reversed(raw_datasets):
            dataset_directory = DirectoryNode(
                Path(self.crate.source) / traversed_dataset.directory
            )
            for on_disk_file in dataset_directory.iter_files(recursive=True):
                if file_filter.exclude(on_disk_file.path()):
                    continue
                file__relative_path = Path(on_disk_file.path()).relative_to(
                    self.crate.source
                )
                if file__relative_path.as_posix() in [
                    datafile.filename for datafile in raw_datafiles
                ]:
                    continue
                raw_datafiles.append(
                    self._process_datafile(
                        file__relative_path,
                        traversed_dataset.description,
                    )
                )
        return raw_datafiles

    def process_datasets(
        self,
        ingestible_classes: IngestibleDataclasses,
        file_filter: filters.PathFilterSet,
    ) -> IngestibleDataclasses:
        """Read in all datasets and datafiles

        Starting at the root dataset (dataset "./")
        move through all child datasets and datafiles
        Children are defined by "has part".
        Then moving upwards from 'leaf' datasets
        load all child datafiles in the filesystem not explicitly listed.


        Args:
            ingestible_classes (IngestibleDataclasses): Ingestible objects to incorporate raw data
            file_filter (filters.PathFilterSet): Filter for specific files (i.e. system files)

        Returns:
            IngestibleDataclasses: Ingestible dataclasses now containing all datafiles and datasets
        """
        datasets_to_read = []
        datasets_to_read.append("./")
        raw_datasets: list[RawDataset] = []
        raw_datafiles: list[RawDatafile] = []
        while len(datasets_to_read) > 0:
            dataset_id = datasets_to_read.pop()
            if self._apply_crate_name(dataset_id) in [
                dataset.description for dataset in raw_datasets
            ]:
                continue
            crate_dataset = self.crate.dereference(dataset_id)
            processed_dataset = self._process_dataset(crate_dataset)
            raw_datasets.append(processed_dataset)

            dataset_parts = crate_dataset.get("hasPart")
            if dataset_parts:
                for child_part in dataset_parts:
                    child_entity: DataEntity = self.crate.dereference(child_part["@id"])
                    if "Dataset" in child_entity.type:
                        if child_entity.id in [
                            dataset.directory for dataset in raw_datasets
                        ]:
                            continue
                        datasets_to_read.append(child_part["@id"])
                    elif "File" in child_entity.type:
                        datafile_id = child_part["@id"]
                        if datafile_id in [
                            datafile.filename for datafile in raw_datafiles
                        ]:
                            continue
                        try:
                            validate_url(datafile_id)
                        except ValueError:
                            raw_datafiles.append(
                                self._process_datafile(
                                    Path(datafile_id),
                                    self._apply_crate_name(dataset_id),
                                )
                            )
        raw_datafiles = self._collect_unlisted_datafiles(
            raw_datasets, raw_datafiles, file_filter
        )
        ingestible_classes.add_datafiles(raw_datafiles)
        ingestible_classes.add_datasets(raw_datasets)

        return ingestible_classes

    def run_extraction(
        self, ingestible_classes: IngestibleDataclasses
    ) -> IngestibleDataclasses:
        file_filter = filters.PathFilterSet(filter_system_files=True)
        ingestible_classes = self.process_projects(
            ingestible_classes=ingestible_classes
        )

        ingestible_classes = self.process_experiments(
            ingestible_classes=ingestible_classes
        )
        ingestible_classes = self.process_datasets(
            ingestible_classes=ingestible_classes, file_filter=file_filter
        )
        return ingestible_classes
