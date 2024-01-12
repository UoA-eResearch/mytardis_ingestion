# pylint: disable=C0103, C0301
"""
Example Usage: python ingestion_biru_example.py \
"/path/to/the/yaml/file" \
"/path/to/the/rsync/destination"

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

# ---Imports
import sys
from pathlib import Path

from src.beneficiations.beneficiation import Beneficiation
from src.config.config import ConfigFromEnv
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport
from src.extraction.extraction_plant import ExtractionPlant
from src.ingestion_factory.factory import IngestionFactory
from src.miners.miner import Miner
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.profiles.profile_loader import ProfileLoader
from src.prospectors.prospector import Prospector
from src.smelters.smelter import Smelter

# Set up logging
logger = logging.getLogger(__name__)

# Constants
config = ConfigFromEnv()
profile_name = "idw"
profile_loader = ProfileLoader(profile_name)


# ---Main
def ingest_data(yaml_pth: Path, rsync_pth: Path) -> None:
    """
    Runs the ingestion pipeline.
    """
    # Extraction Plant
    prospector = Prospector(profile_loader.load_custom_prospector())
    miner = Miner(profile_loader.load_custom_miner())
    beneficiation = Beneficiation(profile_loader.load_custom_beneficiation())

    ext_plant = ExtractionPlant(prospector, miner, beneficiation)
    ingestible_dataclasses = ext_plant.run_extraction(yaml_pth)

    # Ingestion Factory
    mt_rest = MyTardisRESTFactory(config.auth, config.connection)
    overseer = Overseer(mt_rest)

    smelter = Smelter(
        general=config.general,
        default_schema=config.default_schema,
        storage=config.storage,
        overseer=overseer,
    )

    factory = IngestionFactory(
        config=config,
        mt_rest=mt_rest,
        overseer=overseer,
        smelter=smelter,
    )

    factory.ingest_projects(ingestible_dataclasses.get_projects())
    factory.ingest_experiments(ingestible_dataclasses.get_experiments())
    factory.ingest_datasets(ingestible_dataclasses.get_datasets())
    factory.ingest_datafiles(ingestible_dataclasses.get_datafiles())

    # Initiate Conveyor
    datafiles = ingestible_dataclasses.get_datafiles()
    rsync_transport = RsyncTransport(Path(rsync_pth))
    conveyor = Conveyor(rsync_transport)
    conveyor_process = conveyor.initiate_transfer(Path(yaml_pth).parent, datafiles)

    # Wait for file transfer to finish.
    conveyor_process.join()


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
    ingest_data(args.yaml_pth, args.rsync_pth)
