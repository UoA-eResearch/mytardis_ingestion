# pylint: disable=R0801, C0123, fixme
"""IngestionFactory is a base class for specific instances of MyTardis
Ingestion scripts. The base class contains mostly concrete functions but
needs to determine the Smelter class that is used by the Factory"""
import json
import logging
from pathlib import Path
from typing import Any, Optional

from src.blueprints.custom_data_types import URI
from src.blueprints.datafile import Datafile, RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import ConfigFromEnv
from src.conveyor.conveyor import Conveyor, FailedTransferException
from src.crucible.crucible import Crucible
from src.extraction.manifest import IngestionManifest
from src.forges.forge import Forge
from src.mytardis_client.enumerators import ObjectSearchEnum
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


class IngestionResult:  # pylint: disable=missing-class-docstring
    def __init__(
        self,
        success: Optional[list[tuple[str, Optional[URI]]]] = None,
        skipped: Optional[list[tuple[str, Optional[URI]]]] = None,
        error: Optional[list[str]] = None,
    ) -> None:
        self.success = success or []
        self.skipped = skipped or []
        self.error = error or []

    def __eq__(self, other) -> bool:  # type: ignore
        if isinstance(other, IngestionResult):
            return self.success == other.success and self.error == other.error
        return NotImplemented


class IngestionFactory:
    """Orchestrates the ingestion of raw metadata into MyTardis.

    The class runs the smelter-crucible-forge stages of the ingestion process, such that
    the Smelter, Crucible and Forge classes are unaware of each other.

    Attributes:
        mt_rest: client for interacting with MyTardis REST API
        overseer: An instance of the Overseer class
        smelter: used to refine the metadata for ingestion
        crucible: used to prepare the metadata for ingestion
        forge: used to upload the metadata to MyTardis
    """

    def __init__(
        self,
        config: ConfigFromEnv,
        mt_rest: Optional[MyTardisRESTFactory] = None,
        overseer: Optional[Overseer] = None,
        smelter: Optional[Smelter] = None,
        forge: Optional[Forge] = None,
        crucible: Optional[Crucible] = None,
        conveyor: Optional[Conveyor] = None,
    ) -> None:
        """Initialises the IngestionFactory with the given configuration"""

        self.config = config

        mt_rest = mt_rest or MyTardisRESTFactory(config.auth, config.connection)
        self._overseer = overseer or Overseer(mt_rest)

        self.forge = forge or Forge(mt_rest)
        self.smelter = smelter or Smelter(
            overseer=self._overseer,
            general=config.general,
            default_schema=config.default_schema,
        )
        self.crucible = crucible or Crucible(
            overseer=self._overseer,
        )
        self.conveyor = conveyor or Conveyor(store=config.storage)

    def ingest_projects(
        self,
        projects: list[RawProject],
    ) -> IngestionResult:  # sourcery skip: class-extract-method
        """Wrapper function to create the projects from input files"""

        result = IngestionResult()

        for raw_project in projects:
            smelted_project = self.smelter.smelt_project(raw_project)
            if not smelted_project:
                result.error.append(raw_project.display_name)
                continue

            refined_project, refined_parameters = smelted_project

            project = self.crucible.prepare_project(refined_project)
            if not project:
                result.error.append(refined_project.display_name)
                continue

            matching_projects = self._overseer.get_objects(
                ObjectSearchEnum.PROJECT.value, project.name
            )
            if len(matching_projects) > 0:
                project_uri = matching_projects[0]["resource_uri"]
                # Would we ever get multiple matches? If so, what should we do?
                logging.info(
                    'Already ingested project "%s" as "%s". Skipping project ingestion.',
                    project.name,
                    project_uri,
                )
                result.skipped.append((project.display_name, project_uri))
                continue

            project_uri = self.forge.forge_project(project, refined_parameters)
            result.success.append((project.display_name, project_uri))

        return result

    def ingest_experiments(  # pylint: disable=too-many-locals
        self,
        experiments: list[RawExperiment],
    ) -> IngestionResult:
        """Wrapper function to create the experiments from input files"""

        result = IngestionResult()

        for raw_experiment in experiments:
            smelted_experiment = self.smelter.smelt_experiment(raw_experiment)
            if not smelted_experiment:
                result.error.append(raw_experiment.display_name)
                continue

            refined_experiment, refined_parameters = smelted_experiment

            experiment = self.crucible.prepare_experiment(refined_experiment)
            if not experiment:
                result.error.append(refined_experiment.display_name)
                continue

            matching_experiments = self._overseer.get_objects(
                ObjectSearchEnum.EXPERIMENT.value, experiment.title
            )
            if len(matching_experiments) > 0:
                experiment_uri = matching_experiments[0]["resource_uri"]
                logging.info(
                    'Already ingested experiment "%s" as "%s". Skipping experiment ingestion.',
                    experiment.title,
                    experiment_uri,
                )
                result.skipped.append((experiment.display_name, experiment_uri))
                continue

            experiment_uri = self.forge.forge_experiment(experiment, refined_parameters)

            result.success.append((experiment.display_name, experiment_uri))

        return result

    def ingest_datasets(  # pylint: disable=too-many-locals
        self,
        datasets: list[RawDataset],
    ) -> IngestionResult:
        """Wrapper function to create the experiments from input files"""

        result = IngestionResult()

        for raw_dataset in datasets:
            smelted_dataset = self.smelter.smelt_dataset(raw_dataset)
            if not smelted_dataset:
                result.error.append(raw_dataset.display_name)
                continue

            refined_dataset, refined_parameters = smelted_dataset
            dataset = self.crucible.prepare_dataset(refined_dataset)
            if not dataset:
                result.error.append(refined_dataset.display_name)
                continue

            matching_datasets = self._overseer.get_objects(
                ObjectSearchEnum.DATASET.value, dataset.description
            )
            if len(matching_datasets) > 0:
                dataset_uri = matching_datasets[0]["resource_uri"]
                logging.info(
                    'Already ingested dataset "%s" as "%s". Skipping dataset ingestion.',
                    dataset.description,
                    dataset_uri,
                )
                result.skipped.append((dataset.display_name, dataset_uri))
                continue

            dataset_uri = self.forge.forge_dataset(dataset, refined_parameters)
            result.success.append((dataset.display_name, dataset_uri))

        return result

    def ingest_datafiles(
        self,
        source_data_root: Path,
        raw_datafiles: list[RawDatafile],
    ) -> IngestionResult:
        """Wrapper function to create the experiments from input files"""
        result = IngestionResult()
        datafiles: list[Datafile] = []

        for raw_datafile in raw_datafiles:
            refined_datafile = self.smelter.smelt_datafile(raw_datafile)
            if not refined_datafile:
                result.error.append(raw_datafile.display_name)
                continue

            datafile = self.crucible.prepare_datafile(refined_datafile)
            if not datafile:
                result.error.append(refined_datafile.display_name)
                continue
            # Add a replica to represent the copy transferred by the Conveyor.
            datafile.replicas.append(self.conveyor.create_replica(datafile))

            # Search by filename and parent dataset as filenames alone may not be unique
            matching_datafiles = self._overseer.get_objects_by_fields(
                ObjectSearchEnum.DATAFILE.value,
                {
                    "filename": datafile.filename,
                    "directory": datafile.directory.as_posix(),
                    "dataset": str(Overseer.resource_uri_to_id(datafile.dataset)),
                },
            )
            if len(matching_datafiles) > 0:
                logging.info(
                    'Already ingested datafile "%s". Skipping datafile ingestion.',
                    datafile.directory,
                )
                result.skipped.append((datafile.display_name, None))
                continue

            self.forge.forge_datafile(datafile)
            datafiles.append(datafile)
            result.success.append((datafile.display_name, None))

        logger.info(
            "Successfully ingested %d datafile metadata: %s",
            len(result.success),
            result.success,
        )
        if result.error:
            logger.warning(
                "There were errors ingesting %d datafiles: %s",
                len(result.error),
                result.error,
            )

        # Create a file transfer with the conveyor
        logger.info("Starting transfer of datafiles.")
        try:
            self.conveyor.transfer(source_data_root, datafiles)
            logger.info("Finished transferring datafiles.")
        except FailedTransferException:
            logger.error(
                "Datafile transfer could not complete. Check rsync output for more information."
            )
        return result

    def dump_ingestion_result_json(  # pylint:disable=missing-function-docstring
        self,
        projects_result: IngestionResult | None,
        experiments_result: IngestionResult | None,
        datasets_result: IngestionResult | None,
        datafiles_result: IngestionResult | None,
    ) -> None:
        class IngestionResultEncoder(
            json.JSONEncoder
        ):  # pylint: disable=missing-class-docstring
            def default(
                self, o: Any
            ) -> Any:  # pylint:disable=missing-function-docstring
                if isinstance(o, IngestionResult):
                    return o.__dict__
                return json.JSONEncoder.default(self, o)

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
                cls=IngestionResultEncoder,
            )

    def ingest(  # pylint: disable=missing-function-docstring
        self, manifest: IngestionManifest
    ) -> None:
        ingested_projects = self.ingest_projects(manifest.get_projects())
        self.log_results(ingested_projects, "project")

        ingested_experiments = self.ingest_experiments(manifest.get_experiments())
        self.log_results(ingested_experiments, "experiment")

        ingested_datasets = self.ingest_datasets(manifest.get_datasets())
        self.log_results(ingested_datasets, "dataset")

        ingested_datafiles = self.ingest_datafiles(
            manifest.get_data_root(), manifest.get_datafiles()
        )
        self.log_results(ingested_datafiles, "datafile")

        self.dump_ingestion_result_json(
            projects_result=ingested_projects,
            experiments_result=ingested_experiments,
            datasets_result=ingested_datasets,
            datafiles_result=ingested_datafiles,
        )

    def log_results(self, result: IngestionResult, object_type: str) -> None:
        """Logs the details of an ingestion result"""

        logger.info("Finished ingesting %ss", object_type)
        logger.info("%d %ss successfully ingested", len(result.success), object_type)
        logger.info("%d %ss skipped", len(result.skipped), object_type)
        logger.info("%d %ss failed", len(result.error), object_type)

        if result.error:
            logger.error(
                "There were errors ingesting %d %ss: %s",
                len(result.error),
                object_type,
                result.error,
            )
