"""
Ingestion runner for the ABI MuSIC data
"""

import io
import logging
from pathlib import Path

import typer

from src.config.config import ConfigFromEnv
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport
from src.ingestion_factory.factory import IngestionFactory
from src.profiles.profile_register import load_profile
from src.utils import log_utils
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.timing import Timer


def main(
    data_root: Path,
    storage_dir: Path,
    profile_name: str,
    profile_version: str,
    log_file: Path = Path("ingestion.log"),
) -> None:
    """
    Run an ingestion
    """
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    config = ConfigFromEnv()

    timer = Timer(start=True)

    root_dir = DirectoryNode(data_root)
    if root_dir.empty():
        raise ValueError("Data root directory is empty. May not be mounted.")

    profile = load_profile(profile_name, profile_version)

    extractor = profile.get_extractor()
    metadata = extractor.extract(root_dir.path())

    logging.info("Number of datafiles: %d", len(metadata.get_datafiles()))

    # Does this logging still meet our needs?
    stream = io.StringIO()
    metadata.print(stream)
    logging.info(stream.getvalue())

    elapsed = timer.stop()
    logging.info("Finished parsing data directory into PEDD hierarchy")
    logging.info("Total time (s): %.2f", elapsed)

    logging.info("Submitting to MyTardis")
    timer.start()

    ingestion_agent = IngestionFactory(config=config)

    ingestion_agent.ingest(
        metadata.get_projects(),
        metadata.get_experiments(),
        metadata.get_datasets(),
        metadata.get_datafiles(),
    )

    elapsed = timer.stop()
    logging.info("Finished submitting dataclasses to MyTardis")
    logging.info("Total time (s): %.2f", elapsed)

    datafiles = metadata.get_datafiles()
    transport = RsyncTransport(Path(storage_dir))
    conveyor = Conveyor(transport)
    conveyor_process = conveyor.initiate_transfer(data_root, datafiles)
    conveyor_process.join()


if __name__ == "__main__":
    typer.run(main)
