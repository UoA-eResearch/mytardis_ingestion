"""reclaimer.py - Reclaimer cleans up data files that are ingested and verified
in MyTardis.  
"""

import logging
from dataclasses import dataclass
from typing import Any

from src.blueprints.datafile import Datafile, RawDatafile
from src.crucible.crucible import Crucible
from src.mytardis_client.enumerators import ObjectSearchEnum
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


@dataclass
class IngestionStatusResult:
    """Dataclass for ingestion status, separating files that are verified in MyTardis
    and files that are not yet verified."""

    verified_files: list[Datafile] = []
    unverified_files: list[Datafile] = []


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

    def get_ingestion_status(self, raw_dfs: list[RawDatafile]) -> IngestionStatusResult:
        datafiles = self.prepare_datafiles(raw_dfs)
        result = IngestionStatusResult()
        for datafile in datafiles:
            if self.is_file_verified(datafile):
                result.verified_files.append(datafile)
            else:
                result.unverified_files.append(datafile)
        return result

    def prepare_datafiles(self, raw_dfs: list[RawDatafile]) -> list[Datafile]:
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

    def is_file_verified(self, datafile: Datafile) -> bool:
        """Method that filters through a set of Datafiles, returning
        files that are verified on MyTardis.

        Args:
            files (list[Datafile]): A list of datafiles to filter through.

        Returns:
            list[Datafile]: Datafiles which have verified replicas
             on the server.
        """
        result = self._overseer.get_objects_by_fields(
            ObjectSearchEnum.DATAFILE.value,
            {
                "filename": datafile.filename,
                "directory": datafile.directory.as_posix(),
                "dataset": str(Overseer.resource_uri_to_id(datafile.dataset)),
            },
        )
        if len(result) > 0:
            first_df_result = result[0]
            replicas: list[dict[str, Any]] = first_df_result["replicas"]
            for replica in replicas:
                if replica["location"] != self._storage_box_name:
                    continue
                return replica["verified"]
        return False
