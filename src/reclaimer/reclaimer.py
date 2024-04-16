"""reclaimer.py - Reclaimer cleans up data files that are ingested and verified
in MyTardis.  
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Generic, TypeAlias, TypeVar

from src.blueprints.datafile import Datafile, RawDatafile
from src.blueprints.dataset import Dataset, RawDataset
from src.blueprints.experiment import Experiment, RawExperiment
from src.blueprints.project import Project, RawProject
from src.crucible.crucible import Crucible
from src.extraction.manifest import IngestionManifest
from src.mytardis_client.enumerators import MyTardisObject, ObjectSearchEnum
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class QueryResult(Generic[T]):
    object: T
    result: dict[str, Any] | None = None


@dataclass
class IngestionStatusResult:
    """Dataclass for ingestion status, separating files that are verified in MyTardis
    and files that are not yet verified."""

    projects: list[QueryResult[RawProject]]
    experiments: list[QueryResult[RawExperiment]]
    datasets: list[QueryResult[RawDataset]]
    datafiles: list[QueryResult[RawDatafile]]

    def is_complete(self) -> bool:
        """Returns whether this set of ingestion is complete.

        Returns:
            bool: True if all projects, experiments, datasets and datafiles are
        ingested, and all datafiles are verified. False if not.
        """
        return all(
            queryres.result is not None
            for queryres in self.projects
            + self.experiments
            + self.datasets
            + self.datafiles
        ) and all(
            Reclaimer.is_file_verified(datafile.result)
            for datafile in self.datafiles
            if datafile.result is not None
        )


class Reclaimer:
    """Class for checking through files."""

    _storage_box_name: str
    _overseer: Overseer

    def __init__(
        self,
        storage_box_name: str,
        overseer: Overseer,
        smelter: Smelter,
        crucible: Crucible,
    ) -> None:
        self._storage_box_name = storage_box_name
        self._overseer = overseer
        self._smelter = smelter
        self._crucible = crucible

    def get_ingestion_status(
        self, manifest: IngestionManifest
    ) -> IngestionStatusResult:
        """Method that retrieves the ingestion status of the manifest of raw objects.
        Currently, it mainly checks the verification status of the datafiles.

        Args:
            manifest (IngestionManifest): the manifest to be checked.

        Returns:
            IngestionStatusResult: Ingestion status result for the manifest.
        """
        project_status = self._fetch_project_status(manifest.get_projects())
        exp_status = self._fetch_experiment_status(manifest.get_experiments())
        ds_status = self._fetch_dataset_status(manifest.get_datasets())
        df_status = self._fetch_datafile_status(manifest.get_datafiles())
        return IngestionStatusResult(
            projects=project_status,
            experiments=exp_status,
            datasets=ds_status,
            datafiles=df_status,
        )

    def _fetch_project_status(
        self, raw_projects: list[RawProject]
    ) -> list[QueryResult[RawProject]]:
        results: list[QueryResult[RawProject]] = []
        for raw_project in raw_projects:
            res = QueryResult(raw_project)
            results.append(res)
            smelted_project = self._smelter.smelt_project(raw_project)
            if not smelted_project:
                continue
            refined_project, _ = smelted_project
            project = self._crucible.prepare_project(refined_project)
            if not project:
                continue

            matching_projects = self._overseer.get_objects(
                ObjectSearchEnum.PROJECT.value, project.name
            )
            if len(matching_projects) == 0:
                continue
            res.result = matching_projects[0]
        return results

    def _fetch_experiment_status(
        self, raw_experiments: list[RawExperiment]
    ) -> list[QueryResult[RawExperiment]]:
        results: list[QueryResult[RawExperiment]] = []
        for raw_exp in raw_experiments:
            res = QueryResult(raw_exp)
            results.append(res)
            smelted_exp = self._smelter.smelt_experiment(raw_exp)
            if not smelted_exp:
                continue
            refined_exp, _ = smelted_exp
            exp = self._crucible.prepare_experiment(refined_exp)
            if not exp:
                continue

            matching_exps = self._overseer.get_objects(
                ObjectSearchEnum.EXPERIMENT.value, exp.title
            )
            if len(matching_exps) == 0:
                continue
            res.result = matching_exps[0]
        return results

    def _fetch_dataset_status(
        self, raw_datasets: list[RawDataset]
    ) -> list[QueryResult[RawDataset]]:
        results: list[QueryResult[RawDataset]] = []
        for raw_ds in raw_datasets:
            res = QueryResult(raw_ds)
            results.append(res)
            smelted_ds = self._smelter.smelt_dataset(raw_ds)
            if not smelted_ds:
                continue
            refined_ds, _ = smelted_ds
            ds = self._crucible.prepare_dataset(refined_ds)
            if not ds:
                continue

            matching_ds = self._overseer.get_objects(
                ObjectSearchEnum.EXPERIMENT.value, ds.description
            )
            if len(matching_ds) == 0:
                continue
            res.result = matching_ds[0]
        return results

    def _fetch_datafile_status(
        self, raw_dfs: list[RawDatafile]
    ) -> list[QueryResult[RawDatafile]]:
        results: list[QueryResult[RawDatafile]] = []
        for raw_df in raw_dfs:
            res = QueryResult(raw_df)
            results.append(res)
            smelted_df = self._smelter.smelt_datafile(raw_df)
            if not smelted_df:
                continue
            df = self._crucible.prepare_datafile(smelted_df)
            if not df:
                continue

            matching_df = self._overseer.get_objects_by_fields(
                ObjectSearchEnum.DATAFILE.value,
                {
                    "filename": df.filename,
                    "directory": df.directory.as_posix(),
                    "dataset": str(Overseer.resource_uri_to_id(df.dataset)),
                },
            )

            if len(matching_df) == 0:
                continue
            res.result = matching_df[0]
        return results

    def _prepare_datafiles(self, raw_dfs: list[RawDatafile]) -> list[Datafile]:
        datafiles = []
        for raw_df in raw_dfs:
            refined_df = self._smelter.smelt_datafile(raw_df)
            if not refined_df:
                logger.error(
                    "Datafile %s not found", raw_df.directory / raw_df.filename
                )
                continue
            df = self._crucible.prepare_datafile(refined_df)
            if not df:
                logger.error(
                    "Crucible could not generate datafile %s",
                    refined_df.directory / refined_df.filename,
                )
                continue
            datafiles.append(df)
        return datafiles

    @staticmethod
    def is_file_verified(query_result: dict[str, Any]) -> bool:
        """Method that filters through a set of Datafiles, returning
        files that are verified on MyTardis.

        Args:
            files (list[Datafile]): A list of datafiles to filter through.

        Returns:
            list[Datafile]: Datafiles which have verified replicas
             on the server.
        """
        replicas: list[dict[str, Any]] = query_result["replicas"]
        for replica in replicas:
            if replica["verified"]:
                return True
        return False
