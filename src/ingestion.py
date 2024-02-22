"""
CLI frontend for extracting metadata, and ingesting it with the data into MyTardis.
"""

import io
import logging
from pathlib import Path
from typing import Optional

import typer
from typing_extensions import Annotated

from src.config.config import ConfigFromEnv, FilesystemStorageBoxConfig
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport
from src.ingestion_factory.factory import IngestionFactory
from src.profiles.profile_register import load_profile
from src.utils import log_utils
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.timing import Timer


def main(
    data_root: Annotated[
        Path,
        typer.Argument(help="Directory containing the data to be extracted"),
    ],
    storage_name: Annotated[
        str,
        typer.Argument(help="Name of the ingestion storagebox.")
    ],
    storage_dir: Annotated[
        Path,
        typer.Argument(help="Directory of the ingestion storagebox"),
    ],
    profile_name: Annotated[
        str,
        typer.Argument(
            help="Name of the ingestion profile to be used to extract the data"
        ),
    ],
    profile_version: Annotated[
        Optional[str],
        typer.Argument(
            help="Version of the profile to be used. If left unspecified, the latest will be used"
        ),
    ] = None,
    log_file: Annotated[
        Optional[Path],
        typer.Argument(help="Path to be used for the log file"),
    ] = Path("ingestion.log"),
) -> None:
    """
    Run an ingestion
    """
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    config = ConfigFromEnv()

    # Create storagebox config based on passed in argument.
    config.store = FilesystemStorageBoxConfig(storage_name=storage_name, target_root_dir=storage_dir)

    timer = Timer(start=True)

    if DirectoryNode(data_root).empty():
        raise ValueError("Data root directory is empty. May not be mounted.")

    profile = load_profile(profile_name, profile_version)

    extractor = profile.get_extractor()
    metadata = extractor.extract(data_root)

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

    transport = RsyncTransport(Path(storage_dir))
    conveyor = Conveyor(transport)
    conveyor.initiate_transfer(data_root, metadata.get_datafiles()).join()


if __name__ == "__main__":
    typer.run(main)
