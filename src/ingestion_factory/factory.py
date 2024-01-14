# pylint: disable=R0801, C0123, fixme
"""IngestionFactory is a base class for specific instances of MyTardis
Ingestion scripts. The base class contains mostly concrete functions but
needs to determine the Smelter class that is used by the Factory"""

import json
import logging
import sys
from typing import Optional

import aiohttp
from pydantic import ValidationError

from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import ConfigFromEnv
from src.config.singleton import Singleton
from src.crucible.crucible import Crucible
from src.forges.forge import Forge
from src.helpers.dataclass import get_object_name
from src.helpers.enumerators import ObjectSearchEnum
from src.helpers.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


class IngestionResult:  # pylint: disable=missing-class-docstring
    def __init__(
        self,
        success: Optional[list[tuple[str, Optional[URI]]]] = None,
        error: Optional[list[str]] = None,
    ) -> None:
        self.success = success or []
        self.error = error or []

    def __eq__(self, other) -> bool:  # type: ignore
        if isinstance(other, IngestionResult):
            return self.success == other.success and self.error == other.error
        return NotImplemented


class IngestionFactory(metaclass=Singleton):
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
        config: Optional[ConfigFromEnv] = None,
        mt_rest: Optional[MyTardisRESTFactory] = None,
        overseer: Optional[Overseer] = None,
        smelter: Optional[Smelter] = None,
        forge: Optional[Forge] = None,
        crucible: Optional[Crucible] = None,
    ) -> None:
        """Initialises the Factory with the configuration found in the config_dict.

        Passes the config_dict to the overseer and forge instances to ensure that the
        specific MyTardis configuration is shared across all classes

        Args:
            general : GeneralConfig
                Pydantic config class containing general information
            auth : AuthConfig
                Pydantic config class containing information about authenticating with a MyTardis
                instance
            connection : ConnectionConfig
                Pydantic config class containing information about connecting to a MyTardis instance
            mytardis_setup : IntrospectionConfig
                Pydantic config class containing information from the introspection API
            smelter : Smelter
                class instance of Smelter
        """
        if not config:
            try:
                config = ConfigFromEnv()
            except ValidationError as error:
                logger.error(
                    (
                        "An error occurred while validating the environment "
                        "configuration. Make sure all required variables are set "
                        "or pass your own configuration instance. Error: %s"
                    ),
                    error,
                )
                sys.exit()

        mt_rest = mt_rest or MyTardisRESTFactory(config.auth, config.connection)
        self._overseer = overseer or Overseer(mt_rest)

        self.forge = forge or Forge(mt_rest)
        self.smelter = smelter or Smelter(
            overseer=self._overseer,
            general=config.general,
            default_schema=config.default_schema,
            storage=config.storage,
        )
        self.crucible = crucible or Crucible(
            overseer=self._overseer,
            storage=config.storage,
        )

    async def ingest_projects(
        self, projects: list[RawProject] | None, session: aiohttp.ClientSession
    ) -> IngestionResult | None:  # sourcery skip: class-extract-method
        """Wrapper function to create the projects from input files"""
        if not projects:
            return None

        result = IngestionResult()

        for project in projects:
            name = get_object_name(project)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the project, skipping project defined by %s",
                        project,
                    )
                )
                result.error.append("unknown")
                continue

            smelted_project = await self.smelter.smelt_project(project)
            if not smelted_project:
                result.error.append(name)
                continue

            refined_project, refined_parameters = smelted_project

            prepared_project = await self.crucible.prepare_project(
                refined_project, session
            )
            if not prepared_project:
                result.error.append(name)
                continue

            matching_projects = await self._overseer.get_objects(
                ObjectSearchEnum.PROJECT.value, session, prepared_project.name
            )
            if len(matching_projects) == 0:
                project_uri = await self.forge.forge_project(
                    prepared_project, refined_parameters, session
                )
            else:
                project_uri = matching_projects[0]["resource_uri"]
                logging.info(
                    'Already ingested project "%s" as "%s". Skipping project ingestion.',
                    prepared_project.name,
                    project_uri,
                )

            # Note: if the ingestion was skipped because the project already exists,
            #       is that really a "success"?
            result.success.append((name, project_uri))

        logger.info(
            "Successfully ingested %d projects: %s", len(result.success), result.success
        )
        if result.error:
            logger.warning(
                "There were errors ingesting %d projects: %s",
                len(result.error),
                result.error,
            )

        return result

    async def ingest_experiments(  # pylint: disable=too-many-locals
        self, experiments: list[RawExperiment] | None, session: aiohttp.ClientSession
    ) -> IngestionResult | None:
        """Wrapper function to create the experiments from input files"""
        if not experiments:
            return None

        result = IngestionResult()

        for experiment in experiments:
            name = get_object_name(experiment)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the experiment, skipping experiment "
                        "defined by %s",
                        experiment,
                    )
                )
                result.error.append("unknown")
                continue

            smelted_experiment = await self.smelter.smelt_experiment(experiment)
            if not smelted_experiment:
                result.error.append(name)
                continue

            refined_experiment, refined_parameters = smelted_experiment

            prepared_experiment = await self.crucible.prepare_experiment(
                refined_experiment, session
            )
            if not prepared_experiment:
                result.error.append(name)
                continue

            matching_experiments = await self._overseer.get_objects(
                ObjectSearchEnum.EXPERIMENT.value, session, prepared_experiment.title
            )
            if len(matching_experiments) == 0:
                experiment_uri = await self.forge.forge_experiment(
                    prepared_experiment, session, refined_parameters
                )
            else:
                experiment_uri = matching_experiments[0]["resource_uri"]
                logging.info(
                    'Already ingested experiment "%s" as "%s". Skipping experiment ingestion.',
                    prepared_experiment.title,
                    experiment_uri,
                )

            result.success.append((name, experiment_uri))

        logger.info(
            "Successfully ingested %d experiments: %s",
            len(result.success),
            result.success,
        )
        if result.error:
            logger.warning(
                "There were errors ingesting %d experiments: %s",
                len(result.error),
                result.error,
            )

        return result

    async def ingest_datasets(  # pylint: disable=too-many-locals
        self, datasets: list[RawDataset] | None, session: aiohttp.ClientSession
    ) -> IngestionResult | None:
        """Wrapper function to create the experiments from input files"""
        if not datasets:
            return None

        result = IngestionResult()

        for dataset in datasets:
            name = get_object_name(dataset)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the dataset, skipping dataset defined by %s",
                        dataset,
                    )
                )
                result.error.append("unknown")
                continue

            smelted_dataset = self.smelter.smelt_dataset(dataset)
            if not smelted_dataset:
                result.error.append(name)
                continue

            refined_dataset, refined_parameters = smelted_dataset
            prepared_dataset = await self.crucible.prepare_dataset(
                refined_dataset, session
            )
            if not prepared_dataset:
                result.error.append(name)
                continue

            matching_datasets = await self._overseer.get_objects(
                ObjectSearchEnum.DATASET.value, session, prepared_dataset.description
            )
            if len(matching_datasets) == 0:
                dataset_uri = await self.forge.forge_dataset(
                    prepared_dataset, session, refined_parameters
                )
            else:
                dataset_uri = matching_datasets[0]["resource_uri"]
                logging.info(
                    'Already ingested dataset "%s" as "%s". Skipping dataset ingestion.',
                    prepared_dataset.description,
                    dataset_uri,
                )

            result.success.append((name, dataset_uri))

        logger.info(
            "Successfully ingested %d datasets: %s", len(result.success), result.success
        )
        if result.error:
            logger.warning(
                "There were errors ingesting %d datasets: %s",
                len(result.error),
                result.error,
            )

        return result

    async def ingest_datafiles(
        self, datafiles: list[RawDatafile] | None, session: aiohttp.ClientSession
    ) -> IngestionResult | None:
        """Wrapper function to create the experiments from input files"""
        if not datafiles:
            return None

        result = IngestionResult()

        for datafile in datafiles:
            name = get_object_name(datafile)
            if not name:
                logger.warning(
                    (
                        "Unable to find the name of the datafile, skipping datafile defined by %s",
                        datafile,
                    )
                )
                result.error.append("unknown")
                continue

            refined_datafile = self.smelter.smelt_datafile(datafile)
            if not refined_datafile:
                result.error.append(name)
                continue

            prepared_datafile = await self.crucible.prepare_datafile(
                refined_datafile, session
            )
            if not prepared_datafile:
                result.error.append(name)
                continue

            # Search by filename and parent dataset as filenames alone may not be unique
            matching_datafiles = await self._overseer.get_objects_by_fields(
                ObjectSearchEnum.DATAFILE.value,
                session,
                {
                    "filename": prepared_datafile.filename,
                    "dataset": str(
                        Overseer.resource_uri_to_id(prepared_datafile.dataset)
                    ),
                },
            )
            if len(matching_datafiles) == 0:
                await self.forge.forge_datafile(prepared_datafile, session)
                logging.info("Ingested datafile %s", prepared_datafile.directory)
            else:
                logging.info(
                    'Already ingested datafile "%s". Skipping datafile ingestion.',
                    prepared_datafile.directory,
                )

            result.success.append((name, None))

        logger.info(
            "Successfully ingested %d datafiles: %s",
            len(result.success),
            result.success,
        )
        if result.error:
            logger.warning(
                "There were errors ingesting %d datafiles: %s",
                len(result.error),
                result.error,
            )
        return result

    def dump_ingestion_result_json(  # pylint:disable=missing-function-docstring
        self,
        projects_result: IngestionResult | None,
        experiments_result: IngestionResult | None,
        datasets_result: IngestionResult | None,
        datafiles_result: IngestionResult | None,
    ) -> None:
        with open("ingestion_result.json", "w", encoding="utf-8") as file:
            json.dump(
                {
                    "projects": projects_result,
                    "experiments": experiments_result,
                    "datasets": datasets_result,
                    "datafiles": datafiles_result,
                },
                file,
                ensure_ascii=False,
                indent=4,
            )

    async def ingest(  # pylint: disable=missing-function-docstring
        self,
        projects: list[RawProject] | None = None,
        experiments: list[RawExperiment] | None = None,
        datasets: list[RawDataset] | None = None,
        datafiles: list[RawDatafile] | None = None,
    ) -> None:
        async with aiohttp.ClientSession() as session:
            ingested_projects = await self.ingest_projects(projects, session)
            if not ingested_projects:
                logger.error("Fatal error while ingesting projects. Check logs.")

            ingested_experiments = await self.ingest_experiments(experiments, session)
            if not ingested_experiments:
                logger.error("Fatal error ingesting experiments. Check logs.")

            ingested_datasets = await self.ingest_datasets(datasets, session)
            if not ingested_datasets:
                logger.error("Fatal error ingesting datasets. Check logs.")

            ingested_datafiles = await self.ingest_datafiles(datafiles, session)
            if not ingested_datafiles:
                logger.error("Fatal error ingesting datafiles. Check logs.")

        self.dump_ingestion_result_json(
            projects_result=ingested_projects,
            experiments_result=ingested_experiments,
            datasets_result=ingested_datasets,
            datafiles_result=ingested_datafiles,
        )
