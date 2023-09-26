"""Performs pre-ingestion_factory tasks

Pre-ingestion_factory tasks include prospecting, mining, and beneficiation.
"""
# ---Imports
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from src.beneficiations.beneficiation import Beneficiation
from src.config.singleton import Singleton
from src.extraction_output_manager.ingestibles import IngestibleDataclasses
from src.extraction_output_manager.output_manager import OutputManager
from src.miners.miner import Miner
from src.prospectors.prospector import Prospector

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class ExtractionPlant(metaclass=Singleton):
    """Used for extracting data from a given profile and path. Involves prospecting, mining and beneficiating.
    Prospecting is used to check and filter out data and metadata files that can be staged for ingestion.
    Mining is used to generate metadata files that contain fields compatible with the raw dataclasses.
    Beneficiating is used to parse the generated metadata files into the raw dataclasses.
    """

    def __init__(
        self,
        prospector: Union[Prospector, None],
        miner: Union[Miner, None],
        beneficiation: Beneficiation,
    ) -> None:
        """Initializes an ExtractionPlant instance with the given parameters.

        Args:
            prospector (Prospector): The prospector instance.
            miner (Miner): The miner instance.
            beneficiation (Beneficiation): The beneficiation instance.

        Returns:
            None

        """
        self.prospector = prospector
        self.miner = miner
        self.beneficiation = beneficiation

    def run_extraction(
        self, pth: Path, ingest_dict: Optional[Dict[str, list[Any]]] = None
    ) -> IngestibleDataclasses:
        """Runs the full extraction process on the given path and file format. In the absence of a custom prospector in the
        prospector singleton and a custom miner in the miner singleton, the prospector and the miner will be skipped as this
        assumes the IDW was used.

        Args:
            pth (Path): The path of the files.
            ingest_dict (Optional[Dict[str, list[Any]]]): A dictionary containing metadata files to ingest. Used when IDW was used. ---> Libby: used when IDW was not used?

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects.

        Raises:
            Exception: If profile for extraction is not set.
        """
        out_man = OutputManager()
        ing_dclasses = IngestibleDataclasses()

        if self.prospector.custom_prospector:
            out_man = self._prospect(pth, out_man)

        if self.miner.custom_miner:
            out_man = self._mine(pth, out_man)

        if not ingest_dict:
            ingest_dict = out_man.metadata_files_to_ingest_dict
        # ingestible_dataclasses_out = self._beneficiate(ingest_dict, ing_dclasses) # Libby: changed to below
        ingestible_dataclasses_out = self._beneficiate(pth, ing_dclasses)
        return ingestible_dataclasses_out

    def _prospect(
        self,
        pth: Path,
        out_man: OutputManager,
    ) -> OutputManager:
        logger.info("prospecting")
        out_man_fnl: OutputManager = self.prospector.prospect_directory(
            pth, True, out_man
        )
        return out_man_fnl

    def _mine(
        self,
        pth: Path,
        out_man: OutputManager,
    ) -> OutputManager:
        logger.info("mining")
        return self.miner.mine_directory(pth, True, out_man)

    #    def _beneficiate(
    #        self,
    #        ingest_dict: Dict[str, list[Any]],
    #        ing_dclasses: IngestibleDataclasses,
    #    ) -> IngestibleDataclasses:
    #        logger.info("beneficiating")
    #        ingestible_dataclasses = self.beneficiation.beneficiation.beneficiate(ingest_dict, ing_dclasses)
    #        return ingestible_dataclasses

    # Libby: changes to IDW _beneficiate - need to test with ABI-music
    def _beneficiate(
        self,
        pth: Path,
        ing_dclasses: IngestibleDataclasses,
    ) -> IngestibleDataclasses:
        logger.info("beneficiating")
        ingestible_dataclasses = self.beneficiation.beneficiation.beneficiate(
            pth, ing_dclasses
        )
        return ingestible_dataclasses
