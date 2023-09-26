"""IngestionFactory is a base class for specific instances of MyTardis
Ingestion scripts. The base class contains mostly concrete functions but
needs to determine the Smelter class that is used by the Factory"""

import json
import logging
import sys
from typing import Optional

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
        overseer = overseer or Overseer(mt_rest)

        self.forge = forge or Forge(mt_rest)
        self.smelter = smelter or Smelter(
            overseer=overseer,
            general=config.general,
            default_schema=config.default_schema,
            storage=config.storage,
        )
        self.crucible = crucible or Crucible(
            overseer,
            storage=config.storage,
        )

    def ingest_projects(
        self,
        projects: list[RawProject] | None,
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

            smelted_project = self.smelter.smelt_project(project)
            if not smelted_project:
                result.error.append(name)
                continue

            refined_project, refined_parameters = smelted_project

            prepared_project = self.crucible.prepare_project(refined_project)
            if not prepared_project:
                result.error.append(name)
                continue

            forged_project = self.forge.forge_project(
                prepared_project, refined_parameters
            )

            if isinstance(forged_project[0], URI):
                result.success.append((name, forged_project[0]))
            else:
                result.success.append((name, None))

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

    def ingest_experiments(  # pylint: disable=too-many-locals
        self,
        experiments: list[RawExperiment] | None,
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

            smelted_experiment = self.smelter.smelt_experiment(experiment)
            if not smelted_experiment:
                result.error.append(name)
                continue

            refined_experiment, refined_parameters = smelted_experiment

            prepared_experiment = self.crucible.prepare_experiment(refined_experiment)
            if not prepared_experiment:
                result.error.append(name)
                continue

            forged_experiment = self.forge.forge_experiment(
                prepared_experiment, refined_parameters
            )

            if isinstance(forged_experiment[0], URI):
                result.success.append((name, forged_experiment[0]))
            else:
                result.success.append((name, None))

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

    def ingest_datasets(  # pylint: disable=too-many-locals
        self,
        datasets: list[RawDataset] | None,
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
            prepared_dataset = self.crucible.prepare_dataset(refined_dataset)
            if not prepared_dataset:
                result.error.append(name)
                continue

            forged_dataset = self.forge.forge_dataset(
                prepared_dataset, refined_parameters
            )
            if isinstance(forged_dataset[0], URI):
                result.success.append((name, forged_dataset[0]))
            else:
                result.success.append((name, None))

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

    def ingest_datafiles(
        self,
        datafiles: list[RawDatafile] | None,
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

            prepared_datafile = self.crucible.prepare_datafile(refined_datafile)
            if not prepared_datafile:
                result.error.append(name)
                continue

            self.forge.forge_datafile(prepared_datafile)
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

    def ingest(  # pylint: disable=missing-function-docstring
        self,
        projects: list[RawProject] | None = None,
        experiments: list[RawExperiment] | None = None,
        datasets: list[RawDataset] | None = None,
        datafiles: list[RawDatafile] | None = None,
    ) -> None:
        ingested_projects = self.ingest_projects(projects)
        if not ingested_projects:
            logger.error("Fatal error while ingesting projects. Check logs.")

        ingested_experiments = self.ingest_experiments(experiments)
        if not ingested_experiments:
            logger.error("Fatal error ingesting experiments. Check logs.")

        ingested_datasets = self.ingest_datasets(datasets)
        if not ingested_datasets:
            logger.error("Fatal error ingesting datasets. Check logs.")

        ingested_datafiles = self.ingest_datafiles(datafiles)
        if not ingested_datafiles:
            logger.error("Fatal error ingesting datafiles. Check logs.")

        self.dump_ingestion_result_json(
            projects_result=ingested_projects,
            experiments_result=ingested_experiments,
            datasets_result=ingested_datasets,
            datafiles_result=ingested_datafiles,
        )
