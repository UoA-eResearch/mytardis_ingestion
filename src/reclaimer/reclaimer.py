"""reclaimer.py - Reclaimer cleans up data files that are ingested and verified
in MyTardis.  
"""

import logging
from dataclasses import dataclass
from typing import Any

from src.blueprints.datafile import RawDatafile
from src.crucible.crucible import Crucible
from src.mytardis_client.enumerators import DataStatus, ObjectSearchEnum
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


@dataclass
class FileVerificationStatus:
    file: RawDatafile
    is_verified: bool = False


class Reclaimer:
    """Class for checking through files."""

    def __init__(
        self,
        overseer: Overseer,
        smelter: Smelter,
        crucible: Crucible,
    ) -> None:
        self._overseer = overseer
        self._smelter = smelter
        self._crucible = crucible

    def fetch_datafiles_status(
        self, raw_dfs: list[RawDatafile]
    ) -> list[FileVerificationStatus]:
        results: list[FileVerificationStatus] = []
        for raw_df in raw_dfs:
            status, is_verified = self._fetch_datafile_status(raw_df)
            raw_df.data_status = status
            results.append(FileVerificationStatus(raw_df, is_verified))
        return results

    def _fetch_datafile_status(self, raw_df: RawDatafile) -> tuple[DataStatus, bool]:
        smelted_df = self._smelter.smelt_datafile(raw_df)
        if not smelted_df:
            return DataStatus.FAILED, False

        df = self._crucible.prepare_datafile(smelted_df)
        if not df:
            return DataStatus.FAILED, False
        matching_df = self._overseer.get_objects_by_fields(
            ObjectSearchEnum.DATAFILE.value,
            {
                "filename": df.filename,
                "directory": df.directory.as_posix(),
                "dataset": str(Overseer.resource_uri_to_id(df.dataset)),
            },
        )
        if len(matching_df) == 0:
            return DataStatus.READY_FOR_INGESTION, False
        if not getattr(matching_df, "replicas"):
            return DataStatus.INGESTED, False
        replicas: list[dict[str, Any]] = matching_df[0]["replicas"]
        for replica in replicas:
            if replica["verified"]:
                return DataStatus.INGESTED, True
        return DataStatus.INGESTED, False
