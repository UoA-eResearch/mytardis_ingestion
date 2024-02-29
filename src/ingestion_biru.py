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
from typing import Any, List, Union

from src.blueprints.datafile import RawDatafile
from src.blueprints.dataset import RawDataset
from src.blueprints.experiment import RawExperiment
from src.blueprints.project import RawProject
from src.config.config import ConfigFromEnv, FilesystemStorageBoxConfig
from src.ingestion_factory.factory import IngestionFactory
from src.mytardis_client.enumerators import DataStatus
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.profiles.idw.yaml_wrapper import write_to_yaml
from src.profiles.profile_register import load_profile
from src.smelters.smelter import Smelter

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IDSIngestionScript:
    """
    IDS Ingestion Script - BIRU Version.

    This script creates objects in MyTardis using the specified workflow.
    """

    def __init__(self, yaml_path: str, storage_dir: str, storage_name: str):
        """
        Initialize the IDSIngestionScript instance.

        Args:
            yaml_path (str): Path to the YAML file.
            rsync_path (str): Path to the rsync destination.
        """
        self.yaml_path = Path(yaml_path)
        self.config = ConfigFromEnv(
            source_directory=yaml_path,
            storage=FilesystemStorageBoxConfig(
                storage_name=storage_name, target_root_dir=Path(storage_dir)
            )
        )
        self.profile_name = "idw"
        self.profile = load_profile(self.profile_name)
        self.extractor = self.profile.get_extractor()
        self.ingestible_dataclass = self.extractor.extract(self.yaml_path)
        self.factory = self.initialize_factory()

    def initialize_factory(self) -> IngestionFactory:
        """
        Initialize the IngestionFactory.

        Returns:
            IngestionFactory: Initialized IngestionFactory instance.
        """
        return IngestionFactory(
            config=self.config
        )

    def run_ingestion(self) -> None:
        """
        Run the ingestion process.

        Args:
            ingestible_dataclasses (IngestibleDataclasses): Ingestible data classes
            containing data to be updated.
        """
        new_ingested_datafiles: List[Any] = []

        try:
            for project in self.ingestible_dataclass.get_projects():
                self.check_ingest_and_update_status(
                    project, "project", new_ingested_datafiles
                )

            for experiment in self.ingestible_dataclass.get_experiments():
                self.check_ingest_and_update_status(
                    experiment, "experiment", new_ingested_datafiles
                )

            for dataset in self.ingestible_dataclass.get_datasets():
                self.check_ingest_and_update_status(
                    dataset, "dataset", new_ingested_datafiles
                )

            for datafile in self.ingestible_dataclass.get_datafiles():
                self.check_ingest_and_update_status(
                    datafile, "datafile", new_ingested_datafiles
                )
                print(datafile)

        except TimeoutError as e:
            logger.error(e)

        # Write the updated ingestible_dataclasses into a YAML file
        write_to_yaml(self.yaml_path, self.ingestible_dataclass)


    def check_ingest_and_update_status(
        self,
        data_obj: Union[RawProject, RawExperiment, RawDataset, RawDatafile],
        obj_type: str,
        datafile_list: List[RawDatafile],
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
            and data_obj.data_status == DataStatus.INGESTED
        ):
            obj_name_attr = obj_type.lower()
            logger.info("%s %s has already been ingested.", obj_type, obj_name_attr)
            return

        # Ingest the data object
        obj_name_attr = obj_type.lower()
        logger.info("Ingesting %s: %s", obj_type.lower(), obj_name_attr)
        result = getattr(self.factory, f"ingest_{obj_type}s")([data_obj])

        # Update the data status
        if result.success is not None:
            data_obj.data_status = DataStatus.INGESTED
            if obj_type == "datafile":
                if not isinstance(data_obj, RawDatafile):
                    raise TypeError("data_obj must be an instance of RawDatafile")
                datafile_list.append(data_obj)
        else:
            data_obj.data_status = DataStatus.FAILED

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Instrument Data Service(IDS) Ingestion Script - BIRU Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example usage:\n"
            "python ingestion_biru_example.py "
            '"/path/to/the/yaml/file" '
            '"/path/to/the/rsync/destination"'
            '"unix_fs"'
        ),
    )
    parser.add_argument("yaml_pth", type=str, help="Path to the yaml file")
    parser.add_argument("storage_dir", type=str, help="Path to the storage directory")
    parser.add_argument("storage_name", type=str, help="Name of staging storagebox")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()

    script = IDSIngestionScript(args.yaml_pth, args.storage_dir, args.storage_name)
    script.run_ingestion()
