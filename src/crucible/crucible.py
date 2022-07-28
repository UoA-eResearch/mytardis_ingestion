# pylint: disable=duplicate-code,logging-fstring-interpolation
"""The Crucible class takes a refined object and replaces the str values for
fields that exist within the MyTardis Database with their equivalent URIs.
"""
import logging

from src.blueprints.custom_data_types import ISODateTime
from src.blueprints.datafile import Datafile, RefinedDatafile
from src.blueprints.dataset import Dataset, RefinedDataset
from src.blueprints.experiment import Experiment, RefinedExperiment
from src.blueprints.project import Project, RefinedProject
from src.helpers.config import IntrospectionConfig
from src.helpers.enumerators import ObjectSearchEnum
from src.overseers import Overseer

logger = logging.getLogger(__name__)


class Crucible:
    """The Crucible class reads in a RefinedObject and replaces the identified
    fields with URIs."""

    def __init__(
        self, overseer: Overseer, mytardis_setup: IntrospectionConfig
    ) -> None:
        self.overseer = overseer
        if mytardis_setup:
            self.projects_enabled = mytardis_setup.projects_enabled
            self.objects_with_ids = mytardis_setup.objects_with_ids
            self.objects_with_profiles = mytardis_setup.objects_with_profiles

    def refine_project(self, raw_project: RefinedProject) -> Project | None:
        """Refine a project by getting the objects that need to exist in
        MyTardis and finding their URIs"""
        institutions = []
        for institution in raw_project.institution:
            institution_uri = self.overseer.get_uris(
                ObjectSearchEnum.INSTITUTION, institution
            )
            if institution_uri:
                institutions.append(*institution_uri)
        institutions = list(set(institutions))
        if not institutions:
            logger.warning(
                (
                    "Unable to identify any institutions that were "
                    "listed for this project."
                )
            )
            return None
        refined_project = Project(
            name=raw_project.name,
            description=raw_project.description,
            principal_investigator=raw_project.principal_investigator,
            created_by=raw_project.created_by,
            url=raw_project.url,
            users=raw_project.users,
            groups=raw_project.groups,
            persistent_id=raw_project.persistent_id,
            alternate_ids=raw_project.alternate_ids,
            institution=institutions,
        )
        if raw_project.start_time:
            refined_project.start_time = ISODateTime(raw_project.start_time)
        if raw_project.end_time:
            refined_project.end_time = ISODateTime(raw_project.end_time)
        if raw_project.embargo_until:
            refined_project.embargo_until = ISODateTime(
                raw_project.embargo_until
            )
        return refined_project

    def refine_experiment(
        self, raw_experiment: RefinedExperiment
    ) -> Experiment | None:
        """Refine an experiment by getting the objects that need to exist
        in MyTardis and finding their URIs"""
        projects = []
        if raw_experiment.projects and self.projects_enabled:
            for project in raw_experiment.projects:
                project_uri = self.overseer.get_uris(
                    ObjectSearchEnum.PROJECT, project
                )
                if project_uri:
                    projects.append(*project_uri)
            projects = list(set(projects))
        if not projects:
            if self.projects_enabled:
                logger.warning(
                    (
                        "No projects identified for this experiment and "
                        "projects enabled in MyTardis."
                    )
                )
                return None
            projects = None
        refined_experiment = Experiment(
            title=raw_experiment.title,
            description=raw_experiment.description,
            created_by=raw_experiment.created_by,
            url=raw_experiment.url,
            locked=raw_experiment.locked,
            users=raw_experiment.users,
            groups=raw_experiment.groups,
            persistent_id=raw_experiment.persistent_id,
            alternate_ids=raw_experiment.alternate_ids,
            projects=projects,
            institution_name=raw_experiment.institution_name,
        )
        if raw_experiment.start_time:
            refined_experiment.start_time = ISODateTime(
                raw_experiment.start_time
            )
        if raw_experiment.end_time:
            refined_experiment.end_time = ISODateTime(raw_experiment.end_time)
        if raw_experiment.created_time:
            refined_experiment.created_time = ISODateTime(
                raw_experiment.created_time
            )
        if raw_experiment.update_time:
            refined_experiment.update_time = ISODateTime(
                raw_experiment.update_time
            )
        if raw_experiment.embargo_until:
            refined_experiment.embargo_until = ISODateTime(
                raw_experiment.embargo_until
            )
        return refined_experiment

    def refine_dataset(self, raw_dataset: RefinedDataset) -> Dataset | None:
        """Refine a dataset by finding URIs from MyTardis for the
        relevant fields of interest"""
        experiments = []
        for experiment in raw_dataset.experiments:
            experiment_uri = self.overseer.get_uris(
                ObjectSearchEnum.PROJECT, experiment
            )
            if experiment_uri:
                experiments.append(*experiment_uri)
        experiments = list(set(experiments))
        if not experiments:
            logger.warning(
                ("Unable to find experiments associated with this dataset.")
            )
            return None
        instruments = self.overseer.get_uris(
            ObjectSearchEnum.INSTRUMENT, raw_dataset.instrument
        )
        instruments = list(set(instruments))
        if not instruments:
            logger.warning(
                (
                    "Unable to find the instrument associated with this "
                    "dataset in MyTardis."
                )
            )
            return None
        if len(instruments) > 1:
            logger.warning(
                (
                    "Unable to uniquely identify the instrument associated "
                    "with the name or identifier provided. Possible candidates "
                    f"are: {instruments}"
                )
            )
            return None
        instrument = instruments[0]
        refined_dataset = Dataset(
            description=raw_dataset.description,
            directory=raw_dataset.directory,
            users=raw_dataset.users,
            groups=raw_dataset.groups,
            immutable=raw_dataset.immutable,
            persistent_id=raw_dataset.persistent_id,
            alternate_ids=raw_dataset.alternate_ids,
            experiments=experiments,
            instrument=instrument,
        )
        if raw_dataset.created_time:
            refined_dataset.created_time = ISODateTime(
                raw_dataset.created_time
            )
        if raw_dataset.modified_time:
            refined_dataset.modified_time = ISODateTime(
                raw_dataset.modified_time
            )
        return refined_dataset

    def refine_datafile(
        self, raw_datafile: RefinedDatafile
    ) -> Datafile | None:
        """Refine a datafile by finding URIs from MyTardis for the
        relevant fields of interest."""
        datasets = self.overseer.get_uris(
            ObjectSearchEnum.DATASET, raw_datafile.dataset
        )
        if not datasets:
            logger.warning(
                (
                    "Unable to find the dataset associated with "
                    "this datafile in MyTardis."
                )
            )
            return None
        datasets = list(set(datasets))
        if len(datasets) > 1:
            logger.warning(
                (
                    "Unable to uniquely identify the dataset associated "
                    "with this datafile in MyTardis. Possible candidates "
                    f"are: {datasets}"
                )
            )
        dataset = datasets[0]
        refined_datafile = Datafile(
            filename=raw_datafile.filename,
            directory=raw_datafile.directory,
            md5sum=raw_datafile.md5sum,
            mimetype=raw_datafile.mimetype,
            size=raw_datafile.size,
            users=raw_datafile.users,
            groups=raw_datafile.groups,
            dataset=dataset,
            parameter_sets=raw_datafile.parameter_sets,
            replicas=raw_datafile.replicas,
        )
        return refined_datafile
