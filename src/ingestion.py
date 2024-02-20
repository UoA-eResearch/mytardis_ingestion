"""
CLI frontend for extracting metadata, and ingesting it with the data into MyTardis.
"""

import io
import logging
from pathlib import Path
from typing import Optional, TypeAlias

import typer
from typing_extensions import Annotated

from src.config.config import ConfigFromEnv
from src.conveyor.conveyor import Conveyor
from src.conveyor.transports.rsync import RsyncTransport
from src.ingestion_factory.factory import IngestionFactory
from src.profiles.profile_register import load_profile
from src.utils import log_utils
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.timing import Timer

app = typer.Typer()

# =============================================================================
# SHARED ARGUMENT DEFINITIONS
# =============================================================================

SourceDataDirArg: TypeAlias = Annotated[
    Path,
    typer.Argument(help="Directory containing the data to be extracted"),
]

ProfileNameArg: TypeAlias = Annotated[
    str,
    typer.Argument(help="Name of the ingestion profile to be used to extract the data"),
]

ProfileVersionArg = Annotated[
    Optional[str],
    typer.Argument(
        help="Version of the profile to be used. If left unspecified, the latest will be used"
    ),
]

LogFileArg: TypeAlias = Annotated[
    Optional[Path],
    typer.Argument(help="Path to be used for the log file"),
]


# =============================================================================
# COMMANDS
# =============================================================================


@app.command()
def extract(
    data_dir: SourceDataDirArg,
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Directory where the extracted metadata will be stored before ingestion"
        ),
    ],
    profile_name: ProfileNameArg,
    profile_version: ProfileVersionArg = None,
    log_file: LogFileArg = Path("ingestion.log"),
) -> None:
    """
    Extract metadata from a directory tree and parse it into a MyTardis PEDD structure.
    """

    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    timer = Timer(start=True)

    if DirectoryNode(data_dir).empty():
        raise ValueError("Data root directory is empty. May not be mounted.")

    profile = load_profile(profile_name, profile_version)

    extractor = profile.get_extractor()
    metadata = extractor.extract(data_dir)

    logging.info("Number of datafiles: %d", len(metadata.get_datafiles()))

    # Does this logging still meet our needs?
    stream = io.StringIO()
    metadata.print(stream)
    logging.info(stream.getvalue())

    elapsed = timer.stop()
    logging.info("Finished parsing data directory into PEDD hierarchy")
    logging.info("Total time (s): %.2f", elapsed)

    metadata.serialize(output_dir)

    logging.info("Finished. Ingestion manifest written to %s.", output_dir)


@app.command()
def ingest(
    data_root: SourceDataDirArg,
    storage_dir: Annotated[
        Path,
        typer.Argument(help="Directory where the extracted data will be stored"),
    ],
    profile_name: ProfileNameArg,
    profile_version: ProfileVersionArg = None,
    log_file: LogFileArg = Path("ingestion.log"),
) -> None:
    """
    Run the full ingestion process, from extracting metadata to ingesting it into MyTardis.
    """
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    config = ConfigFromEnv()

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
    app()
