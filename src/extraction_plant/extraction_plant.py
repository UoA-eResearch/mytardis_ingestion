"""Performs pre-ingestion_factory tasks

Pre-ingestion_factory tasks include prospecting, mining, and beneficiation.
"""
# ---Imports
import logging
from typing import Optional

from src.beneficiations.beneficiation import Beneficiation
from src.miners.miner import Miner
from src.profiles import profile_consts as pc
from src.profiles.output_manager import OutputManager
from src.prospectors.prospector import Prospector
from src.utils.ingestibles import IngestibleDataclasses


# ---Constants
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# ---Code
class ExtractionPlant:
    """Used for extracting data from a given profile and path. Involves prospecting, mining and beneficiating.
    Prospecting is used to check and filter out data and metadata files that can be staged for ingestion.
    Mining is used to generate metadata files that contain fields compatible with the raw dataclasses.
    Beneficiating is used to parse the generated metadata files into the raw dataclasses.
    """

    def __init__(
        self,
        profile: Optional[str] = None,
        prospector: Optional[Prospector] = None,
        miner: Optional[Miner] = None,
        beneficiation: Optional[Beneficiation] = None,
    ) -> None:
        """Initializes an ExtractionPlant instance with the given parameters.

        Args:
            profile (str, optional): The profile name. Defaults to None.
            prospector (Prospector, optional): The prospector instance. Defaults to None.
            miner (Miner, optional): The miner instance. Defaults to None.
            beneficiation (Beneficiation, optional): The beneficiation instance. Defaults to None.

        Returns:
            None

        """
        self.profile = profile

        if profile:
            self.propsector = prospector if prospector else Prospector(profile)

            self.miner = miner if miner else Miner(profile)

        self.beneficiation = beneficiation if beneficiation else Beneficiation()

    def run_extraction(
        self,
        pth: str,
        file_frmt: str,
    ) -> IngestibleDataclasses:
        """Runs the full extraction process on the given path and file format.

        Args:
            pth (str): The path of the files.
            file_frmt (str): The file format of the metadata.

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects.

        Raises:
            Exception: If profile for extraction is not set.
        """
        if self.profile:
            out_man = self._prospect(self.profile, pth)
            out_man = self._mine(self.profile, pth, out_man)
        else:
            logger.error("Profile not set")
            raise Exception("Profile for extraction not set")

        ingest_dict = out_man.metadata_files_to_ingest_dict
        ingestible_dataclasses = self._beneficiate(ingest_dict, file_frmt)
        return ingestible_dataclasses

    def run_extraction_with_IDW(
        self,
        ingest_dict: dict[str, list[str]],
        file_frmt: str,
    ) -> IngestibleDataclasses:
        """Runs extraction process on the given path and file format after using the IDW.

        Args:
            ingest_dict (dict[str, list[str]]): A dictionary of ingest files.
            file_frmt (str): The file format of the metadata.

        Returns:
            IngestibleDataclasses: A class that contains the raw datafiles, datasets, experiments, and projects.
        """
        ingestible_dataclasses = self._beneficiate(ingest_dict, file_frmt)
        return ingestible_dataclasses

    def _prospect(
        self,
        profile: str,
        pth: str,
    ) -> OutputManager:
        logger.info("prospecting")
        prospector = Prospector(profile)
        return prospector.prospect_directory(pth)

    def _mine(self, profile: str, pth: str, out_man: OutputManager) -> OutputManager:
        logger.info("mining")
        miner = Miner(profile)
        return miner.mine_directory(pth, True, out_man)

    def _beneficiate(
        self,
        ingest_dict: dict[str, list[str]],
        file_format: str,
    ) -> IngestibleDataclasses:
        logger.info("beneficiating")
        proj_files = ingest_dict[pc.PROJECT_NAME]
        expt_files = ingest_dict[pc.EXPERIMENT_NAME]
        dset_files = ingest_dict[pc.DATASET_NAME]
        dfile_files = ingest_dict[pc.DATAFILE_NAME]
        ingestible_dataclasses = self.beneficiation.beneficiate(
            proj_files, expt_files, dset_files, dfile_files, file_format
        )
        return ingestible_dataclasses
