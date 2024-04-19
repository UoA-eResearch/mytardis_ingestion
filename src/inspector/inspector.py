# pylint: disable=duplicate-code
"""insepctor.py - Inspector queries MyTardis to check the status of ingestion.  
"""

import logging
from typing import Any

from src.blueprints.datafile import RawDatafile
from src.config.config import ConfigFromEnv
from src.crucible.crucible import Crucible
from src.mytardis_client.enumerators import ObjectSearchEnum
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.smelters.smelter import Smelter

logger = logging.getLogger(__name__)


class Inspector:
    """Class for querying the status of ingestion process."""

    def __init__(self, config: ConfigFromEnv) -> None:
        mt_rest = MyTardisRESTFactory(config.auth, config.connection)
        overseer = Overseer(mt_rest)
        smelter = Smelter(
            overseer=overseer,
            general=config.general,
            default_schema=config.default_schema,
        )
        crucible = Crucible(overseer)
        self._overseer = overseer
        self._smelter = smelter
        self._crucible = crucible

    def is_datafile_verified(self, raw_df: RawDatafile) -> bool:
        """Queries MyTardis for the verification status of the given raw datafile.

        Args:
            raw_df (RawDatafile): The raw datafile to check.

        Returns:
            bool: True if the datafile is verified, False if not.
        """
        smelted_df = self._smelter.smelt_datafile(raw_df)
        if not smelted_df:
            return False
        df = self._crucible.prepare_datafile(smelted_df)
        if not df:
            return False
        # Look up the datafile in MyTardis to check if it's ingested.
        matching_df = self._overseer.get_objects_by_fields(
            ObjectSearchEnum.DATAFILE.value,
            {
                "filename": df.filename,
                "directory": df.directory.as_posix(),
                "dataset": str(Overseer.resource_uri_to_id(df.dataset)),
            },
        )
        if len(matching_df) == 0:
            return False
        if not matching_df[0]["replicas"]:
            return False
        replicas: list[dict[str, Any]] = matching_df[0]["replicas"]
        # Iterate through all replicas. If one replica is verified, then
        # return True.
        for replica in replicas:
            if replica["verified"]:
                return True
        return False
