# pylint: disable=C0301

"""Performs pre-ingestion_factory tasks

Pre-ingestion_factory tasks include prospecting, mining, and beneficiation.
"""
# ---Imports
import logging
from pathlib import Path
from typing import Optional

from src.beneficiations.beneficiation import Beneficiation
from src.extraction.manifest import IngestionManifest
from src.extraction.metadata_extractor import IMetadataExtractor
from src.extraction.output_manager import OutputManager
from src.miners.miner import Miner
from src.prospectors.prospector import Prospector

# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class ExtractionPlant(IMetadataExtractor):
    """An implementation of the IMetadataExtractor interface, designed for the case where
    metadata extraction has been implemented in separate "Prospect->Mine->Beneficiate" stages,
    or a subset of those.

    - Prospecting is used to check and filter out data and metadata files that can be staged for
    ingestion.
    - Mining is used to generate metadata files that contain fields compatible with the raw
    dataclasses.
    - Beneficiating is used to parse the generated metadata files into the raw dataclasses.

    Note: If these more granular stages aren't needed, it is preferable to produce a separate
    implementation of IMetadataExtractor directly.
    """

    def __init__(
        self,
        prospector: Optional[Prospector],
        miner: Optional[Miner],
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

    def extract(self, root_dir: Path) -> IngestionManifest:
        """Runs the full extraction process on the given path and file format. In the absence of a custom prospector in the
        prospector singleton and a custom miner in the miner singleton, the prospector and the miner will be skipped as this
        assumes the IDW was used.

        Args:
            pth (Path): The path of the files.
            ingest_dict (Optional[Dict[str, list[Any]]]): A dictionary containing metadata files to ingest. Used when IDW was used. ---> Libby: used when IDW was not used?

        Returns:
            IngestionManifest: A class that contains the raw datafiles, datasets, experiments, and projects.

        Raises:
            Exception: If profile for extraction is not set.
        """
        out_man = OutputManager()

        if self.prospector and self.prospector.custom_prospector:
            out_man = self._prospect(root_dir, out_man)

        if self.miner and self.miner.custom_miner:
            out_man = self._mine(root_dir, out_man)

        ingestible_dataclasses_out = self._beneficiate(root_dir)
        return ingestible_dataclasses_out

    def _prospect(
        self,
        pth: Path,
        out_man: OutputManager,
    ) -> OutputManager:
        if self.prospector:
            logger.info("prospecting")
            out_man_fnl: OutputManager = self.prospector.prospect_directory(
                str(pth), True, out_man
            )
            return out_man_fnl
        logger.info("prospector not set")
        return OutputManager()

    def _mine(
        self,
        pth: Path,
        out_man: OutputManager,
    ) -> OutputManager:
        if self.miner:
            logger.info("mining")
            return self.miner.mine_directory(str(pth), True, out_man)
        logger.info("miner not set")
        return OutputManager()

    #    def _beneficiate(
    #        self,
    #        ingest_dict: Dict[str, list[Any]],
    #        ing_dclasses: IngestionManifest,
    #    ) -> IngestionManifest:
    #        logger.info("beneficiating")
    #        ingestible_dataclasses = self.beneficiation.beneficiation.beneficiate(ingest_dict, ing_dclasses)
    #        return ingestible_dataclasses

    # Libby: changes to IDW _beneficiate - need to test with ABI-music
    def _beneficiate(self, pth: Path) -> IngestionManifest:
        logger.info("beneficiating")
        ingestible_dataclasses = self.beneficiation.beneficiation.beneficiate(pth)
        return ingestible_dataclasses
