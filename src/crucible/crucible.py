"""The Crucible class takes a refined object and replaces the str values for
fields that exist within the MyTardis Database with their equivalent URIs.
"""

import logging
from datetime import datetime

from src.blueprints.datafile import Datafile, RefinedDatafile
from src.blueprints.dataset import Dataset, RefinedDataset
from src.blueprints.experiment import Experiment, RefinedExperiment
from src.blueprints.project import Project, RefinedProject
from src.mytardis_client.endpoints import URI
from src.mytardis_client.objects import MyTardisObject
from src.overseers.overseer import Overseer
from src.utils.types.type_helpers import forward_none

logger = logging.getLogger(__name__)


@forward_none
def normalize_datetime(value: datetime | str) -> str:
    """Normalize a datetime or string to a string."""
    return value.isoformat() if isinstance(value, datetime) else value


class Crucible:
    """The Crucible class reads in a RefinedObject and replaces the identified
    fields with URIs."""

    def __init__(self, overseer: Overseer) -> None:
        self.overseer = overseer

    def prepare_project(self, refined_project: RefinedProject) -> Project | None:
        """Refine a project by getting the objects that need to exist in
        MyTardis and finding their URIs"""

        institutions: set[URI] = set()

        for institution in refined_project.institution:
            if institution_uris := self.overseer.get_uris_by_identifier(
                MyTardisObject.INSTITUTION, institution
            ):
                institutions.update(institution_uris)

        if not institutions:
            logger.warning(
                "Unable to identify any institutions that were listed for this project."
            )
            return None

        return Project(
            name=refined_project.name,
            description=refined_project.description,
            data_classification=refined_project.data_classification,
            principal_investigator=refined_project.principal_investigator,
            created_by=refined_project.created_by,
            url=refined_project.url,
            users=refined_project.users,
            groups=refined_project.groups,
            identifiers=refined_project.identifiers,
            institution=list(institutions),
            start_time=normalize_datetime(refined_project.start_time),
            end_time=normalize_datetime(refined_project.end_time),
            embargo_until=normalize_datetime(refined_project.embargo_until),
        )

    def prepare_experiment(
        self, refined_experiment: RefinedExperiment
    ) -> Experiment | None:
        """Refine an experiment by getting the objects that need to exist
        in MyTardis and finding their URIs"""
        projects: set[URI] = set()
        if (
            refined_experiment.projects
            and self.overseer.mytardis_setup.projects_enabled
        ):
            for project_identifier in refined_experiment.projects:
                if project_uris := self.overseer.get_uris_by_identifier(
                    MyTardisObject.PROJECT, project_identifier
                ):
                    projects.update(project_uris)

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
            projects=list(projects),
            institution_name=refined_experiment.institution_name,
            start_time=normalize_datetime(refined_experiment.start_time),
            end_time=normalize_datetime(refined_experiment.end_time),
            created_time=normalize_datetime(refined_experiment.created_time),
            update_time=normalize_datetime(refined_experiment.update_time),
            embargo_until=normalize_datetime(refined_experiment.embargo_until),
        )

    def prepare_dataset(self, refined_dataset: RefinedDataset) -> Dataset | None:
        """Refine a dataset by finding URIs from MyTardis for the
        relevant fields of interest"""
        experiment_uris: set[URI] = set()

        for experiment_identifier in refined_dataset.experiments:
            if uris := self.overseer.get_uris_by_identifier(
                MyTardisObject.EXPERIMENT, experiment_identifier
            ):
                experiment_uris.update(uris)
            else:
                logger.warning("No URI found for experiment %s.", experiment_identifier)

        # experiment_uris = list(set(experiment_uris))
        if not experiment_uris:
            logger.warning("Unable to find experiments associated with this dataset.")
            return None

        instrument_uris = self.overseer.get_uris_by_identifier(
            MyTardisObject.INSTRUMENT, refined_dataset.instrument
        )
        if len(instrument_uris) == 0:
            logger.warning(
                "Unable to find the instrument associated with this dataset in MyTardis."
            )
            return None
        if len(instrument_uris) > 1:
            logger.warning(
                (
                    "Unable to uniquely identify the instrument associated with the name or "
                    "identifier provided. Possible candidates are: %s"
                ),
                instrument_uris,
            )
            return None
        instrument_uri = instrument_uris[0]

        return Dataset(
            description=refined_dataset.description,
            directory=refined_dataset.directory,
            data_classification=refined_dataset.data_classification,
            users=refined_dataset.users,
            groups=refined_dataset.groups,
            immutable=refined_dataset.immutable,
            identifiers=refined_dataset.identifiers,
            experiments=list(experiment_uris),
            instrument=instrument_uri,
            created_time=normalize_datetime(refined_dataset.created_time),
            modified_time=normalize_datetime(refined_dataset.modified_time),
        )

    def prepare_datafile(self, refined_datafile: RefinedDatafile) -> Datafile | None:
        """Refine a datafile by finding URIs from MyTardis for the
        relevant fields of interest."""

        dataset_uris = self.overseer.get_uris_by_identifier(
            MyTardisObject.DATASET, refined_datafile.dataset
        )
        if len(dataset_uris) == 0:
            logger.warning(
                "Unable to find the dataset associated with this datafile in MyTardis."
            )
            return None
        if len(dataset_uris) > 1:
            logger.warning(
                (
                    "Unable to uniquely identify the dataset associated with this datafile "
                    "in MyTardis. Possible candidates are: %s"
                ),
                dataset_uris,
            )
            return None

        dataset_uri = dataset_uris[0]

        return Datafile(
            filename=refined_datafile.filename,
            directory=refined_datafile.directory,
            md5sum=refined_datafile.md5sum,
            mimetype=refined_datafile.mimetype,
            size=refined_datafile.size,
            users=refined_datafile.users,
            groups=refined_datafile.groups,
            dataset=dataset_uri,
            parameter_sets=refined_datafile.parameter_sets,
            replicas=[],
        )
