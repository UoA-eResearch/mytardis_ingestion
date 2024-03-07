"""
CLI frontend for extracting metadata, and ingesting it with the data into MyTardis.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, TypeAlias

import typer
from pydantic import ValidationError
from typing_extensions import Annotated

from src.config.config import ConfigFromEnv, FilesystemStorageBoxConfig
from src.extraction.manifest import IngestionManifest
from src.ingestion_factory.factory import IngestionFactory
from src.profiles.profile_register import load_profile
from src.utils import log_utils
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.timing import Timer

app = typer.Typer()
logger = logging.getLogger(__name__)

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
    log_file: LogFileArg = Path("extraction.log"),
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

    elapsed = timer.stop()
    logging.info("Finished parsing data directory into PEDD hierarchy")
    logging.info("Total time (s): %.2f", elapsed)
    logging.info(metadata.summarize())

    metadata.serialize(output_dir)

    logging.info("Extraction complete. Ingestion manifest written to %s.", output_dir)


@app.command()
def upload(
    manifest_dir: Annotated[
        Path,
        typer.Argument(
            help="Directory containing the previously extracted metadata to be ingested"
        ),
    ],
    storage_name: Annotated[
        str, typer.Argument(help="Name of the staging storagebox.")
    ],
    data_dir: SourceDataDirArg,
    storage_dir: Annotated[
        Path,
        typer.Argument(help="Directory of the staging storagebox"),
    ],
    log_file: LogFileArg = Path("upload.log"),
) -> None:
    """
    Submit the extracted metadata to MyTardis, and transfer the data to the storage directory.
    """
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    try:
        config = ConfigFromEnv(
            # Create storagebox config based on passed in argument.
            source_directory=data_root,
            storage=FilesystemStorageBoxConfig(
                storage_name=storage_name, target_root_dir=storage_dir
            ),
        )
    except ValidationError as error:
        logger.error(
            (
                "An error occurred while validating the environment "
                "configuration. Make sure all required variables are set "
                "or pass your own configuration instance. Error: %s"
            ),
            error,
        )
        sys.exit(1)

    if DirectoryNode(manifest_dir).empty():
        raise ValueError(
            "Manifest directory is empty. Extract data into a manifest using 'extract' command."
        )

    metadata = IngestionManifest.deserialize(manifest_dir)

    logging.info("Successfully loaded metadata manifest from %s", manifest_dir)
    logging.info("Submitting metadata to MyTardis")

    timer = Timer(start=True)

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

@app.command()
def ingest(
    data_root: SourceDataDirArg,
    storage_name: Annotated[
        str, typer.Argument(help="Name of the staging storagebox.")
    ],
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
    try:
        config = ConfigFromEnv(
            # Create storagebox config based on passed in argument.
            source_directory=data_root,
            storage=FilesystemStorageBoxConfig(
                storage_name=storage_name, target_root_dir=storage_dir
            ),
        )
    except ValidationError as error:
        logger.error(
            (
                "An error occurred while validating the environment "
                "configuration. Make sure all required variables are set "
                "or pass your own configuration instance. Error: %s"
            ),
            error,
        )
        sys.exit(1)
    timer = Timer(start=True)

    if DirectoryNode(data_root).empty():
        raise ValueError("Data root directory is empty. May not be mounted.")

    profile = load_profile(profile_name, profile_version)

    extractor = profile.get_extractor()
    metadata = extractor.extract(data_root)

    elapsed = timer.stop()
    logging.info("Finished parsing data directory into PEDD hierarchy")
    logging.info("Total time (s): %.2f", elapsed)
    logging.info(metadata.summarize())

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
    logger.info("Finished submitting dataclasses and transferring files to MyTardis")
    logger.info("Total time (s): %.2f", elapsed)


if __name__ == "__main__":
    app()
