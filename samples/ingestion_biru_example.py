# pylint: disable=C0103, C0301, W0511
"""
NB: THIS WILL NOT WORK AS IT IS AND IS PROVIDED FOR INDICATIVE PURPOSES ONLY

Script to create the objects in MyTardis.

The workflow for these scripts are as follows:
-Extraction Plant
 -- Prospector: involves checking the files and metadata to
    see which files are worthy/unworthy of ingestion into MyTardis. Please
    note that this is not compulsory if the IME is used.
 -- Miner: involves converting and standardising the metadata so
    that it can be beneficiated. Please note that this is not compulsory if the
    IME is used.
 -- Beneficiating the metadata: involves parsing the mined or IME-produced
    metadata into raw dataclasses.

-Conveyor
   Taking datafile metadata from the extraction plant, we generate a list of
   of files to transfer into MyTardis, then move them using a configured
   Transport. This happens in a parallel process to the ingestion factory.

-Ingestion Factory # Not working as intended yet, currently using the components separately
 -- Smelting: Refines the raw dataclasses into refined dataclasses which closely
    follows the MyTardis's data formats.
 -- Crucible: Swaps out directory paths in the metadata into URI's. Used mainly
    for locating datafiles in MyTardis's object store.
 -- Forges: Creates MyTardis objects, from the URI-converted dataclasses,
    in the MyTardis database.

For the extraction plant processes, there are two pathways that depends on
whether the IME is used. The rest of the steps remain the same from
beneficiation onwards.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Union

from src.beneficiations.beneficiation import Beneficiation
from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import ConfigFromEnv
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport
from src.extraction.extraction_plant import ExtractionPlant
from src.extraction.ingestibles import IngestibleDataclasses
from src.ingestion_factory.factory import IngestionFactory
from src.miners.miner import Miner
from src.mytardis_client.enumerators import DataStatus
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.profiles.profile_loader import ProfileLoader
from src.prospectors.prospector import Prospector
from src.smelters.smelter import Smelter

# from src.utils.data_status_update import YAMLUpdater

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IDSIngestionScript:
    """
    IDS Ingestion Script - BIRU Version.

    This script creates objects in MyTardis using the specified workflow.
    """

    def __init__(self, yaml_path: str, rsync_path: str):
        """
        Initialize the IDSIngestionScript instance.

        Args:
            yaml_path (str): Path to the YAML file.
            rsync_path (str): Path to the rsync destination.
        """
        self.yaml_path = Path(yaml_path)
        self.rsync_path = Path(rsync_path)
        self.config = ConfigFromEnv()
        self.profile_name = "idw"
        self.profile_loader = ProfileLoader(self.profile_name)
        self.factory = self.initialize_factory()

    def initialize_factory(self) -> IngestionFactory:
        """
        Initialize the IngestionFactory.

        Returns:
            IngestionFactory: Initialized IngestionFactory instance.
        """
        mt_rest = MyTardisRESTFactory(self.config.auth, self.config.connection)
        overseer = Overseer(mt_rest)
        smelter = Smelter(
            general=self.config.general,
            default_schema=self.config.default_schema,
            storage=self.config.storage,
            overseer=overseer,
        )
        return IngestionFactory(
            config=self.config, mt_rest=mt_rest, overseer=overseer, smelter=smelter
        )

    def run_extract(self) -> IngestibleDataclasses:
        """
        Run the extraction plant process.

        Returns:
            IngestibleDataclasses: Result of the extraction plant process.
        """
        prospector = Prospector(self.profile_loader.load_custom_prospector())
        miner = Miner(self.profile_loader.load_custom_miner())
        beneficiation = Beneficiation(self.profile_loader.load_custom_beneficiation())
        ext_plant = ExtractionPlant(prospector, miner, beneficiation)
        ingestibles = ext_plant.run_extraction(self.yaml_path)
        return ingestibles

    def run_ingestion(self, ingestible_dataclass: IngestibleDataclasses) -> None:
        """
        Run the ingestion process.

        Args:
            ingestible_dataclasses (IngestibleDataclasses): Ingestible data classes
            containing data to be updated.
        """
        new_ingested_datafiles = []

        try:
            for project in ingestible_dataclass.get_projects():
                self.update_data_status(project, "project", new_ingested_datafiles)

            for experiment in ingestible_dataclass.get_experiments():
                self.update_data_status(
                    experiment, "experiment", new_ingested_datafiles
                )

            for dataset in ingestible_dataclass.get_datasets():
                self.update_data_status(dataset, "dataset", new_ingested_datafiles)

            for datafile in ingestible_dataclass.get_datafiles():
                self.update_data_status(datafile, "datafile", new_ingested_datafiles)

        except TimeoutError as e:
            logger.error(e)

        # TODO: write the updated ingestible_dataclasses into a YAML file
        # YAMLUpdater(self.yaml_path.as_posix(), ingestible_dataclass)

        # Initiate Conveyor
        self.initiate_conveyor(new_ingested_datafiles)

    def update_data_status(
        self,
        data_obj: Union[RawProject, RawExperiment, RawDataset, RawDatafile],
        obj_type: str,
        datafile_list: list,
    ) -> None:
        """
        Update the data status for a specific object type.

        Args:
            data_obj (Union[RawProject, RawExperiment, RawDataset, RawDatafile]):
                The data object to update.
            obj_type (str): The type of the data object.
            new_ingested_datafiles (list): List to store newly ingested datafiles.
        """
        if (
            data_obj.data_status is not None
            and data_obj.data_status.value == DataStatus.INGESTED.value
        ):
            obj_name_attr = obj_type.lower()
            logger.info("%s %s has already been ingested.", obj_type, obj_name_attr)
            return

        obj_name_attr = obj_type.lower()
        logger.info("Ingesting %s: %s", obj_type.lower(), obj_name_attr)
        result = getattr(self.factory, f"ingest_{obj_type}s")([data_obj])

        # Update the data status
        if result.success is not None:
            data_obj.data_status = DataStatus.INGESTED
            if obj_type == "datafile":
                datafile_list.append(data_obj)
        else:
            data_obj.data_status = DataStatus.FAILED

    def initiate_conveyor(self, new_ingested_datafiles: list) -> None:
        """
        Initiate the Conveyor for file transfer.

        Args:
            new_ingested_datafiles (list): List of newly ingested datafiles.
        """
        logger.info("Initiating the Conveyor for file transfer...")
        rsync_transport = RsyncTransport(self.rsync_path)
        conveyor = Conveyor(rsync_transport)
        conveyor_process = conveyor.initiate_transfer(
            self.yaml_path.parent, new_ingested_datafiles
        )  # only rsync the newly ingested datafiles

        # Wait for file transfer to finish.
        conveyor_process.join()

        logger.info("Data ingestion process completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Instrument Data Service(IDS) Ingestion Script - BIRU Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example usage:\n"
            "python ingestion_biru_example.py "
            '"/path/to/the/yaml/file" '
            '"/path/to/the/rsync/destination"'
        ),
    )
    parser.add_argument("yaml_pth", type=str, help="Path to the yaml file")
    parser.add_argument("rsync_pth", type=str, help="Path to the rsync destination")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()

    script = IDSIngestionScript(args.yaml_pth, args.rsync_pth)
    ingestible_dataclasses = script.run_extract()
    script.run_ingestion(ingestible_dataclasses)
