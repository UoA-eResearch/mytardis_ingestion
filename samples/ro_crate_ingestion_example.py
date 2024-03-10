# pylint: disable=missing-module-docstring, missing-function-docstring, redefined-outer-name

import argparse
import logging
import sys
from pathlib import Path

from src.config.config import ConfigFromEnv
from src.ingestion_factory.factory import IngestionFactory
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.profiles.profile_loader import ProfileLoader
from src.profiles.ro_crate.ro_crate_parser import ROCrateExtractor
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


def ingest_data(ro_crate_path: Path) -> None:
    """
    Runs the ingestion pipeline.
    """
    crate_extractor = ROCrateExtractor()
    metadata = crate_extractor.extract(ro_crate_path)
    # Extraction Plant

    # Ingestion Factory
    mt_rest = MyTardisRESTFactory(config.auth, config.connection)
    overseer = Overseer(mt_rest)

    smelter = Smelter(
        general=config.general,
        default_schema=config.default_schema,
        overseer=overseer,
    )

    factory = IngestionFactory(
        config=config,
        mt_rest=mt_rest,
        overseer=overseer,
        smelter=smelter,
    )

    factory.ingest_projects(metadata.get_projects())
    factory.ingest_experiments(metadata.get_experiments())
    factory.ingest_datasets(metadata.get_datasets())
    factory.ingest_datafiles(metadata.get_datafiles())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Instrument Data Service(IDS) Ingestion Script - RO-Crate Version",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Example usage:\n"
            "python ingestion_ro_crate_example.py "
            '"/path/to/ro_crate/data" '
            '"/path/to/the/rsync/destination"'
        ),
    )
    parser.add_argument("ro_crate_path", type=str, help="Path to the ro_crate file")
    parser.add_argument("rsync_pth", type=str, help="Path to the rsync destination")

    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    args = parser.parse_args()
    ingest_data(args.ro_crate_path)
