# pylint: disable=duplicate-code
"""insepctor.py - Inspector queries MyTardis to check the status of ingestion.
"""

import logging
from typing import Optional

from typeguard import check_type

from src.blueprints.datafile import RawDatafile
from src.config.config import ConfigFromEnv
from src.crucible.crucible import Crucible
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.mytardis_client.objects import MyTardisObject
from src.mytardis_client.response_data import IngestedDatafile
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
        self._overseer: Overseer = overseer
        self._smelter = smelter
        self._crucible = crucible

    def query_datafile(self, raw_df: RawDatafile) -> Optional[list[IngestedDatafile]]:
        """Partially ingests raw datafile and queries MyTardis for matching instances.

        Args:
            raw_df (RawDatafile): The raw datafile to query.

        Returns:
            Optional[list[IngestedDatafile]]: A list of matching datafiles, or None if the raw
            datafile is unable to be smelted or prepared.
        """
        smelted_df = self._smelter.smelt_datafile(raw_df)
        if not smelted_df:
            return None
        df = self._crucible.prepare_datafile(smelted_df)
        if not df:
            return None

        # Look up the datafile in MyTardis to check if it's ingested.
        matches = self._overseer.get_matching_objects(
            MyTardisObject.DATAFILE, df.model_dump()
        )

        return check_type(matches, list[IngestedDatafile])
