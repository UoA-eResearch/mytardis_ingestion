"""Smelter base class. A class that provides functions to split raw dataclasses into
refined dataclasses suited to be passed into an instance of the Crucible class for
matching existing objects in MyTardis.

In addition to splitting the raw dataclasses up it also injects some default values
where appropriate"""

import logging
from typing import Optional, Tuple

from pydantic import ValidationError

from src.blueprints.common_models import Parameter, ParameterSet
from src.blueprints.datafile import RawDatafile, RefinedDatafile
from src.blueprints.dataset import RawDataset, RefinedDataset
from src.blueprints.experiment import RawExperiment, RefinedExperiment
from src.blueprints.project import RawProject, RefinedProject
from src.config.config import GeneralConfig, SchemaConfig
from src.mytardis_client.common_types import MTUrl
from src.mytardis_client.endpoints import URI
from src.overseers.overseer import MYTARDIS_PROJECTS_DISABLED_MESSAGE, Overseer

logger = logging.getLogger(__name__)


class Smelter:
    """The Smelter base class to be subclassed into individual concrete classes for different
    ingestion approaches.
    Smelter classes share a number of similar processing routines, especially around dictionary
    modification and date handling.
    Attributes:
        overseer (Overseer): an instance of the Overseer class that provides access to MyTardis
            and is used to query the introspection API - will be deprecated by SSV
        general (GeneralConfig): an instance of a GeneralConfig class that provides basic
            information for the ingestion
        default_schema (SchemaConfig): default metadata schema to use in cases where no schema
            is provided - will be deprecated by SSV
    """

    def __init__(
        self,
        overseer: Overseer,
        general: GeneralConfig,
        default_schema: SchemaConfig,
    ) -> None:
        """Class initialisation to set options for dictionary processing
        Stores MyTardis set up information from the introspection API to allow the parser
        to prepare only objects that exist and to handle additional keys in the dictionaries
        gracefully.
        Args:
            overseer (Overseer): An instance of the Overseer class for inspecting what is present in
                MyTardis
            general (GeneralConfig): A general configuration as a catch all for config that doesn't
                belong elsewhere
            default_schema (SchemaConfig): The default schema to use as a fallback if one is not
                provided.
        """

        self.overseer = overseer
        self.default_schema = default_schema
        self.default_institution = general.default_institution

    def extract_parameters(
        self,
        schema: MTUrl | URI,
        raw_object: RawProject | RawExperiment | RawDataset | RawDatafile,
    ) -> Optional[ParameterSet]:
        """Extract the metadata field and the schema field from a RawObject dataclass
        and use it to create a Parameterset

        Args:
            schema (AnyURL): a schema namespace from MyTardis
            raw_object (RawMTObject): A RawProject/Experiment/Dataset/Datafile object

        Returns:
            Optional(ParameterSet): The metadata processed into a parameterset
        """

        if not raw_object.metadata:
            return None
        parameter_list: list[Parameter] = []
        parameter = raw_object.metadata
        for key, value in parameter.items():
            try:
                parameter_list.append(Parameter(name=key, value=value))
            except ValidationError:
                logger.warning(
                    ("Unable to parse parameter %s: %s into Parameter", key, value),
                    exc_info=True,
                )
                continue
        return ParameterSet(schema=schema, parameters=parameter_list)

    def smelt_project(
        self, raw_project: RawProject
    ) -> Tuple[RefinedProject, Optional[ParameterSet]] | None:
        """Inject the schema into the project dictionary if it's not
        already present. Do the same for an institution and convert to
        a RawProject dataclass for validation."""
        if not self.overseer.mytardis_setup.projects_enabled:
            logger.warning(MYTARDIS_PROJECTS_DISABLED_MESSAGE)
            return None
        schema = raw_project.object_schema or self.default_schema.project
        if not schema:
            logger.warning(
                "Unable to find default project schema and no schema provided"
            )
            return None
        if raw_project.institution:
            institution = raw_project.institution
        elif self.default_institution:
            institution = [self.default_institution]
        else:
            institution = None
        if not institution:
            logger.warning(
                "Unable to find default institution and no institution provided"
            )
            return None
        try:
            refined_project = RefinedProject(
                name=raw_project.name,
                description=raw_project.description,
                data_classification=raw_project.data_classification,
                principal_investigator=raw_project.principal_investigator,
                created_by=raw_project.created_by,
                url=raw_project.url,
                users=raw_project.users,
                groups=raw_project.groups,
                identifiers=raw_project.identifiers,
                institution=institution,
                start_time=raw_project.start_time,
                end_time=raw_project.end_time,
                embargo_until=raw_project.embargo_until,
            )
        except ValidationError:
            logger.warning(
                (
                    "Malformed project dataclass produced by smelter. Project is %s",
                    raw_project.name,
                ),
                exc_info=True,
            )
            return None
        parameters = self.extract_parameters(MTUrl(schema), raw_project)
        return (refined_project, parameters)

    def smelt_experiment(
        self, raw_experiment: RawExperiment
    ) -> Tuple[RefinedExperiment, Optional[ParameterSet]] | None:
        """Inject the schema into the experiment dictionary if it's not
        already present.
        Convert to a RawExperiment dataclass for validation."""
        schema = raw_experiment.object_schema or self.default_schema.experiment
        if not schema:
            logger.warning(
                "Unable to find default experiment schema and no schema provided"
            )
            return None
        if (
            self.overseer.mytardis_setup.projects_enabled
            and not raw_experiment.projects
        ):  # test this
            logger.warning(
                "Projects enabled in MyTardis and no projects provided to link this experiment to. Experiment provided %s",  # pylint: disable=line-too-long
                raw_experiment,
            )
            return None
        institution_name = raw_experiment.institution_name or self.default_institution
        if not institution_name:
            logger.warning(
                "Unable to find default institution and no institution provided"
            )
            return None
        try:
            refined_experiment = RefinedExperiment(
                title=raw_experiment.title,
                description=raw_experiment.description,
                data_classification=raw_experiment.data_classification,
                created_by=raw_experiment.created_by,
                url=raw_experiment.url,
                locked=raw_experiment.locked,
                users=raw_experiment.users,
                groups=raw_experiment.groups,
                identifiers=raw_experiment.identifiers,
                projects=raw_experiment.projects,
                institution_name=institution_name,
                start_time=raw_experiment.start_time,
                end_time=raw_experiment.end_time,
                created_time=raw_experiment.created_time,
                update_time=raw_experiment.update_time,
                embargo_until=raw_experiment.embargo_until,
            )
        except ValidationError:
            logger.warning(
                (
                    "Malformed experiment dataclass produced by smelter. Experiment is %s",
                    raw_experiment.title,
                ),
                exc_info=True,
            )
            return None
        parameters = self.extract_parameters(schema, raw_experiment)
        return (refined_experiment, parameters)

    def smelt_dataset(
        self, raw_dataset: RawDataset
    ) -> Tuple[RefinedDataset, Optional[ParameterSet]] | None:
        """Inject the schema into the dataset dictionary if it's not
        already present.
        Convert to a RefinedDataset dataclass for validation."""
        schema = raw_dataset.object_schema or self.default_schema.dataset
        if not schema:
            logger.warning(
                "Unable to find default dataset schema and no schema provided"
            )
            return None
        try:
            refined_dataset = RefinedDataset(
                description=raw_dataset.description,
                directory=raw_dataset.directory,
                data_classification=raw_dataset.data_classification,
                users=raw_dataset.users,
                groups=raw_dataset.groups,
                immutable=raw_dataset.immutable,
                identifiers=raw_dataset.identifiers,
                experiments=raw_dataset.experiments,
                instrument=raw_dataset.instrument,
                created_time=raw_dataset.created_time,
                modified_time=raw_dataset.modified_time,
            )
        except ValidationError:
            logger.warning(
                (
                    "Malformed dataset dataclass produced by smelter. Dataset is %s",
                    raw_dataset.description,
                ),
                exc_info=True,
            )
            return None
        parameters = self.extract_parameters(schema, raw_dataset)
        return (refined_dataset, parameters)

    def smelt_datafile(  # pylint: disable=too-many-return-statements
        self, raw_datafile: RawDatafile
    ) -> RefinedDatafile | None:
        """Inject the schema into the datafile dictionary if it's not
        already present.
        Convert to a RefinedDatafile dataclass for validation."""
        schema = raw_datafile.object_schema or self.default_schema.datafile
        if not schema:
            logger.warning(
                "Unable to find default datafile schema and no schema provided"
            )
            return None
        parameters = self.extract_parameters(schema, raw_datafile)
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
                parameter_sets=[parameters] if parameters is not None else None,
            )
        except ValidationError:
            logger.warning(
                (
                    "Malformed datafile dataclass produced by smelter. Datafile "
                    "is %s",
                    raw_datafile.filename,
                ),
                exc_info=True,
            )
            return None
        return refined_datafile
