# pylint: disable=fixme
"""The Crucible class takes a refined object and replaces the str values for
fields that exist within the MyTardis Database with their equivalent URIs.
"""
import logging
from datetime import datetime

from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import Datafile, RefinedDatafile
from src.blueprints.dataset import Dataset, RefinedDataset
from src.blueprints.experiment import Experiment, RefinedExperiment
from src.blueprints.project import Project, RefinedProject
from src.mytardis_client.enumerators import MyTardisObjectType
from src.overseers.overseer import Overseer

logger = logging.getLogger(__name__)


class Crucible:
    """The Crucible class reads in a RefinedObject and replaces the identified
    fields with URIs."""

    def __init__(self, overseer: Overseer) -> None:
        self.overseer = overseer

    def prepare_project(self, refined_project: RefinedProject) -> Project | None:
        """Refine a project by getting the objects that need to exist in
        MyTardis and finding their URIs"""
        institutions: list[URI] = []
        for institution in refined_project.institution:
            if institution_uri := self.overseer.get_uris_by_identifier(
                MyTardisObjectType.INSTITUTION, institution
            ):
                institutions.append(*institution_uri)
        institutions = list(set(institutions))
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
            for project_identifier in refined_experiment.projects:
                if project_uri := self.overseer.get_uris_by_identifier(
                    MyTardisObjectType.PROJECT, project_identifier
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
        for experiment_identifier in refined_dataset.experiments:
            if uris := self.overseer.get_uris_by_identifier(
                MyTardisObjectType.EXPERIMENT, experiment_identifier
            ):
                experiment_uris.extend(uris)
            else:
                logger.warning("No URI found for experiment %s.", experiment_identifier)

        experiment_uris = list(set(experiment_uris))
        if not experiment_uris:
            logger.warning("Unable to find experiments associated with this dataset.")
            return None
        instruments = self.overseer.get_uris_by_identifier(
            MyTardisObjectType.INSTRUMENT, refined_dataset.instrument
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

    def prepare_datafile(self, refined_datafile: RefinedDatafile) -> Datafile | None:
        """Refine a datafile by finding URIs from MyTardis for the
        relevant fields of interest."""
        datasets = self.overseer.get_uris_by_identifier(
            MyTardisObjectType.DATASET, refined_datafile.dataset
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
            replicas=[],
        )
