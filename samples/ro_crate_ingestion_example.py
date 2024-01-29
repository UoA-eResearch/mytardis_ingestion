# pylint: disable=missing-module-docstring, missing-function-docstring, redefined-outer-name

import argparse
import logging
import sys
from pathlib import Path

from src.config.config import ConfigFromEnv
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport
from src.extraction.ingestibles import IngestibleDataclasses
from src.ingestion_factory.factory import IngestionFactory
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.profiles.profile_loader import ProfileLoader
from src.profiles.ro_crate.ro_crate_parser import ROCrateParser
from src.smelters.smelter import Smelter

root = logging.getLogger()
root.setLevel(logging.DEBUG)

file_handler = logging.FileHandler(filename="upload_example.log", mode="w")
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
console_handler.setFormatter(logging.Formatter("[%(levelname)s]: %(message)s"))

root.addHandler(file_handler)
root.addHandler(console_handler)

# Set up logging
logger = logging.getLogger(__name__)

# Constants
config = ConfigFromEnv()
PROFILE_NAME = "ro_crate"
profile_loader = ProfileLoader(PROFILE_NAME)


def ingest_data(
    ro_crate_path: Path, rsync_pth: Path, crate_to_tardis_schema: Path
) -> None:
    """
    Runs the ingestion pipeline.
    """
    crate_parser = ROCrateParser(
        ro_crate_path, crate_to_tardis_schema, schema_config=config.default_schema
    )
    # Extraction Plant
    ingestible_dataclasses = IngestibleDataclasses()
    ingestible_dataclasses = crate_parser.run_extraction(ingestible_dataclasses)

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

    datafiles = ingestible_dataclasses.get_datafiles()
    rsync_transport = RsyncTransport(Path(rsync_pth))
    conveyor = Conveyor(rsync_transport)
    conveyor_process = conveyor.initiate_transfer(Path(ro_crate_path).parent, datafiles)

    #    Wait for file transfer to finish.
    conveyor_process.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Instrument Data Service(IDS) Ingestion Script - RO-Crate Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example usage:\n"
            "python ingestion_ro_crate_example.py "
            '"/path/to/ro_crate/data" '
            '"/path/to/the/rsync/destination"'
            '"/path/to/the/schema/mapping"'
        ),
    )
    parser.add_argument("ro_crate_path", type=str, help="Path to the ro_crate file")
    parser.add_argument("rsync_pth", type=str, help="Path to the rsync destination")
    parser.add_argument(
        "schema_path", type=str, help="Path to the ro_crate to my tardis mapping schema"
    )

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()
    ingest_data(args.ro_crate_path, args.rsync_pth, args.schema_path)
