# pylint: disable=fixme
"""The Crucible class takes a refined object and replaces the str values for
fields that exist within the MyTardis Database with their equivalent URIs.
"""
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Tuple

from slugify import slugify

from src.blueprints.custom_data_types import URI, ISODateTime
from src.blueprints.datafile import Datafile, DatafileReplica, RefinedDatafile
from src.blueprints.dataset import Dataset, RefinedDataset
from src.blueprints.experiment import Experiment, RefinedExperiment
from src.blueprints.project import Project, RefinedProject
from src.blueprints.storage_boxes import RawStorageBox
from src.helpers.config import StorageConfig
from src.helpers.enumerators import ObjectSearchEnum
from src.overseers import Overseer

logger = logging.getLogger(__name__)


### PASTED CODE


class Crucible:
    """The Crucible class reads in a RefinedObject and replaces the identified
    fields with URIs."""

    def __init__(
        self,
        overseer: Overseer,
        active_stores: StorageConfig,
        archive: StorageConfig,
    ) -> None:
        self.overseer = overseer
        self.active_stores = active_stores
        self.archive = archive

    def prepare_project(self, refined_project: RefinedProject) -> Project | None:
        """Refine a project by getting the objects that need to exist in
        MyTardis and finding their URIs"""
        institutions: list[URI] = []
        for institution in refined_project.institution:
            if institution_uri := self.overseer.get_uris(
                ObjectSearchEnum.INSTITUTION.value, institution
            ):
                institutions.append(*institution_uri)
        institutions = list(set(institutions))
        if not institutions:
            logger.warning(
                "Unable to identify any institutions that were listed for this project."
            )
            return None
        active_stores = []
        archives = []
        for store in refined_project.active_stores:
            attributes = self.active_stores.attributes or {}
            if "type" not in attributes:
                attributes["type"] = "disk"
            active_stores.append(
                RawStorageBox(
                    name=store,
                    storage_class=self.active_stores.storage_class,
                    options=self.active_stores.options,
                    attributes=attributes,
                )
            )
        for archive in refined_project.archives:
            attributes = self.archive.attributes or {}
            if "type" not in attributes:
                attributes["type"] = "tape"
            archives.append(
                RawStorageBox(
                    name=archive,
                    storage_class=self.archive.storage_class,
                    options=self.archive.options,
                    attributes=attributes,
                )
            )
        return Project(
            name=refined_project.name,
            autoarchive_offset=refined_project.autoarchive_offset,
            delete_offset=refined_project.delete_offset,
            description=refined_project.description,
            data_classification=refined_project.data_classification,
            principal_investigator=refined_project.principal_investigator,
            created_by=refined_project.created_by,
            url=refined_project.url,
            users=refined_project.users,
            groups=refined_project.groups,
            identifiers=refined_project.identifiers,
            institution=institutions,
            start_time=(
                refined_project.start_time.isoformat()
                if isinstance(refined_project.start_time, datetime)
                else refined_project.start_time
            ),
            end_time=(
                refined_project.end_time.isoformat()
                if isinstance(refined_project.end_time, datetime)
                else refined_project.end_time
            ),
            embargo_until=(
                refined_project.embargo_until.isoformat()
                if isinstance(refined_project.embargo_until, datetime)
                else refined_project.embargo_until
            ),
            archives=archives,
            active_stores=active_stores,
        )

    def prepare_experiment(
        self, refined_experiment: RefinedExperiment
    ) -> Experiment | None:
        """Refine an experiment by getting the objects that need to exist
        in MyTardis and finding their URIs"""
        projects: list[URI] = []
        if (
            refined_experiment.projects
            and self.overseer.mytardis_setup.projects_enabled
        ):
            for project in refined_experiment.projects:
                if project_uri := self.overseer.get_uris(
                    ObjectSearchEnum.PROJECT.value, project
                ):
                    projects.append(*project_uri)
            projects = list(set(projects))
        if not projects and self.overseer.mytardis_setup.projects_enabled:
            logger.warning(
                "No projects identified for this experiment and projects enabled in MyTardis."
            )
            return None
        return Experiment(
            title=refined_experiment.title,
            description=refined_experiment.description,
            data_classification=refined_experiment.data_classification,
            created_by=refined_experiment.created_by,
            url=refined_experiment.url,
            locked=refined_experiment.locked,
            users=refined_experiment.users,
            groups=refined_experiment.groups,
            identifiers=refined_experiment.identifiers,
            projects=projects,
            institution_name=refined_experiment.institution_name,
            start_time=(
                refined_experiment.start_time.isoformat()
                if isinstance(refined_experiment.start_time, datetime)
                else refined_experiment.start_time
            ),
            end_time=(
                refined_experiment.end_time.isoformat()
                if isinstance(refined_experiment.end_time, datetime)
                else refined_experiment.end_time
            ),
            created_time=(
                refined_experiment.created_time.isoformat()
                if isinstance(refined_experiment.created_time, datetime)
                else refined_experiment.created_time
            ),
            update_time=(
                refined_experiment.update_time.isoformat()
                if isinstance(refined_experiment.update_time, datetime)
                else refined_experiment.update_time
            ),
            embargo_until=(
                refined_experiment.embargo_until.isoformat()
                if isinstance(refined_experiment.embargo_until, datetime)
                else refined_experiment.embargo_until
            ),
        )

    def prepare_dataset(self, refined_dataset: RefinedDataset) -> Dataset | None:
        """Refine a dataset by finding URIs from MyTardis for the
        relevant fields of interest"""
        experiment_uris: list[URI] = []
        for experiment in refined_dataset.experiments:
            if uris := self.overseer.get_uris(
                ObjectSearchEnum.EXPERIMENT.value, experiment
            ):
                experiment_uris.extend(uris)
        experiment_uris = list(set(experiment_uris))
        if not experiment_uris:
            logger.warning("Unable to find experiments associated with this dataset.")
            return None
        instruments = self.overseer.get_uris(
            ObjectSearchEnum.INSTRUMENT.value, refined_dataset.instrument
        )
        if instruments:
            instruments = list(set(instruments))
        else:
            logger.warning(
                "Unable to find the instrument associated with this dataset in MyTardis."
            )
            return None
        if len(instruments) > 1:
            logger.warning(
                (
                    "Unable to uniquely identify the instrument associated with the name or "
                    "identifier provided. Possible candidates are: %s"
                ),
                instruments,
            )
            return None
        instrument = instruments[0]

        return Dataset(
            description=refined_dataset.description,
            directory=refined_dataset.directory,
            data_classification=refined_dataset.data_classification,
            users=refined_dataset.users,
            groups=refined_dataset.groups,
            immutable=refined_dataset.immutable,
            identifiers=refined_dataset.identifiers,
            experiments=experiment_uris,
            instrument=instrument,
            created_time=(
                refined_dataset.created_time.isoformat()
                if isinstance(refined_dataset.created_time, datetime)
                else refined_dataset.created_time
            ),
            modified_time=(
                refined_dataset.modified_time.isoformat()
                if isinstance(refined_dataset.modified_time, datetime)
                else refined_dataset.modified_time
            ),
        )

    def __create_datafile_replicas(
        self,
        dataset: URI,
        file_path: Path,
    ) -> Tuple[List[DatafileReplica], int, int]:  # sourcery skip: for-append-to-extend
        """Use the dataset associated with datafile to construct replicas"""
        if response := self.overseer.get_object_by_uri(dataset):
            dataset_obj = response["objects"][0]
            # Should be handled by API - create setting to prefix
            # instrument = slugify(dataset_obj["instrument"]["name"])
            # facility = slugify(dataset_obj["instrument"]["facility"]["name"])
            # mid_path = Path(facility) / Path(instrument)
            replicas = []
            max_active_offset = 0
            max_delete_offset = 0
            for experiment in dataset_obj["experiments"]:
                for project in experiment["projects"]:
                    if project["autoarchive"]["offset"] > max_active_offset:
                        max_active_offset = project["autoarchive"]["offset"]
                    if (
                        project["autoarchive"]["delete_offset"] == -1
                        or max_delete_offset == -1
                    ):
                        max_delete_offset = -1
                    elif project["autoarchive"]["delete_offset"] > max_delete_offset:
                        max_delete_offset = project["autoarchive"]["delete_offset"]
                    for archive in project["autoarchive"]["archives"]:
                        replicas.append(
                            DatafileReplica(
                                uri=file_path.as_posix(),
                                location=archive["name"],
                                protocol="file",
                            ),
                        )
                    for store in project["autoarchive"]["active_stores"]:
                        replicas.append(
                            DatafileReplica(
                                uri=file_path.as_posix(),
                                location=store["name"],
                                protocol="file",
                            ),
                        )

            return (replicas, max_active_offset, max_delete_offset)
        raise ValueError(f"Unable to find dataset: {dataset}")

    def __generate_date(
        self,
        offset: int,
    ) -> ISODateTime:
        """Add an offset to datetime.now and return as an ISODateTime obj."""
        return ISODateTime((datetime.now() + timedelta(days=offset)).isoformat())

    def prepare_datafile(self, refined_datafile: RefinedDatafile) -> Datafile | None:
        """Refine a datafile by finding URIs from MyTardis for the
        relevant fields of interest."""
        datasets = self.overseer.get_uris(
            ObjectSearchEnum.DATASET.value, refined_datafile.dataset
        )
        if not datasets:
            logger.warning(
                "Unable to find the dataset associated with this datafile in MyTardis."
            )
            return None
        datasets = list(set(datasets))
        if len(datasets) > 1:
            logger.warning(
                (
                    "Unable to uniquely identify the dataset associated with this datafile "
                    "in MyTardis. Possible candidates are: %s"
                ),
                datasets,
            )
            return None
        dataset = datasets[0]
        file_path = Path(refined_datafile.directory)
        replicas, offset, delete_offset = self.__create_datafile_replicas(
            dataset, file_path
        )
        archive_date = self.__generate_date(offset)
        print(archive_date)
        delete_date = self.__generate_date(delete_offset)
        # TODO revisit the logic here to see if we need to push this out to individual DFOs
        return Datafile(
            filename=refined_datafile.filename,
            directory=refined_datafile.directory,
            md5sum=refined_datafile.md5sum,
            mimetype=refined_datafile.mimetype,
            size=refined_datafile.size,
            users=refined_datafile.users,
            groups=refined_datafile.groups,
            dataset=dataset,
            parameter_sets=refined_datafile.parameter_sets,
            replicas=replicas,
            archive_date=archive_date,
            delete_date=delete_date,
        )
