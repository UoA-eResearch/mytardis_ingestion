"""
CLI frontend for extracting metadata, and ingesting it with the data into MyTardis.
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional, TypeAlias

import typer
from pydantic import ValidationError
from typing_extensions import Annotated

from src.config.config import ConfigFromEnv, FilesystemStorageBoxConfig
from src.extraction.manifest import IngestionManifest
from src.ingestion_factory.factory import IngestionFactory
from src.inspector.inspector import Inspector
from src.profiles.profile_register import load_profile
from src.utils import log_utils
from src.utils.filesystem.ctime import max_ctime
from src.utils.filesystem.filesystem_nodes import DirectoryNode
from src.utils.timing import Timer

app = typer.Typer()
logger = logging.getLogger(__name__)

# =============================================================================
# SHARED PARAMETER DEFINITIONS
# =============================================================================

SourceDataPathArg: TypeAlias = Annotated[
    Path,
    typer.Argument(
        help="Path to the source data (either a directory or file)",
        exists=True,
        dir_okay=True,
        file_okay=True,
    ),
]

ProfileNameOption: TypeAlias = Annotated[
    str,
    typer.Option(
        "--profile",
        "-p",
        help="Name of the ingestion profile to be used to extract the data",
    ),
]

ProfileVersionOption = Annotated[
    Optional[str],
    typer.Option(
        help="Version of the profile to be used. If left unspecified, the latest will be used",
    ),
]

LogFileOption: TypeAlias = Annotated[
    Path,
    typer.Option(help="Path to be used for the log file"),
]

StorageBoxOption: TypeAlias = Annotated[
    Optional[tuple[str, Path]],
    typer.Option(
        help="Name and filesystem directory of staging StorageBox to transfer datafiles into."
    ),
]


# ==============================================================================
# SHARED FUNCTIONS
# ==============================================================================
def get_config(storage: StorageBoxOption) -> ConfigFromEnv:
    try:
        config = ConfigFromEnv()
        if storage is not None:
            # Create storagebox config based on passed in argument.
            store = FilesystemStorageBoxConfig(
                storage_name=storage[0], target_root_dir=storage[1]
            )
            config.storage = store
        return config
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


# =============================================================================
# COMMANDS
# =============================================================================


@app.command()
def extract(
    source_data_path: SourceDataPathArg,
    output_dir: Annotated[
        Path,
        typer.Argument(
            help="Directory where the extracted metadata will be stored before ingestion"
        ),
    ],
    profile_name: ProfileNameOption,
    profile_version: ProfileVersionOption = None,
    log_file: LogFileOption = Path("extraction.log"),
) -> None:
    """
    Extract metadata from a directory tree, parse it into a MyTardis PEDD structure,
    and write it to a directory.

    The 'upload' command be used to ingest this extracted data into MyTardis.
    """

    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    timer = Timer(start=True)

    profile = load_profile(profile_name, profile_version)

    extractor = profile.get_extractor()
    metadata = extractor.extract(source_data_path)

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
            help="Directory containing the previously extracted metadata to be ingested",
            exists=True,
            dir_okay=True,
            file_okay=False,
        ),
    ],
    storage: StorageBoxOption = None,
    log_file: LogFileOption = Path("upload.log"),
) -> None:
    """
    Submit the extracted metadata to MyTardis, and transfer the data to the storage directory.
    """
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    config = get_config(storage)
    if DirectoryNode(manifest_dir).empty():
        raise ValueError(
            "Manifest directory is empty. Extract data into a manifest using 'extract' command."
        )

    manifest = IngestionManifest.deserialize(manifest_dir)

    logging.info("Successfully loaded metadata manifest from %s", manifest_dir)
    logging.info("Submitting metadata to MyTardis")

    timer = Timer(start=True)

    ingestion_agent = IngestionFactory(config=config)

    ingestion_agent.ingest(manifest)

    elapsed = timer.stop()
    logging.info("Finished submitting dataclasses to MyTardis")
    logging.info("Total time (s): %.2f", elapsed)


@app.command()
def ingest(
    source_data_path: SourceDataPathArg,
    profile_name: ProfileNameOption,
    storage: StorageBoxOption = None,
    profile_version: ProfileVersionOption = None,
    log_file: LogFileOption = Path("ingestion.log"),
) -> None:
    """
    Run the full ingestion process, from extracting metadata to ingesting it into MyTardis.
    """
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    config = get_config(storage)
    timer = Timer(start=True)

    profile = load_profile(profile_name, profile_version)

    extractor = profile.get_extractor()
    manifest = extractor.extract(source_data_path)

    elapsed = timer.stop()
    logging.info("Finished parsing data directory into PEDD hierarchy")
    logging.info("Total time (s): %.2f", elapsed)
    logging.info(manifest.summarize())

    logging.info("Submitting to MyTardis")
    timer.start()

    ingestion_agent = IngestionFactory(config=config)

    ingestion_agent.ingest(manifest)

    elapsed = timer.stop()
    logger.info("Finished submitting dataclasses and transferring files to MyTardis")
    logger.info("Total time (s): %.2f", elapsed)


@app.command()
def clean(
    source_data_path: SourceDataPathArg,
    profile_name: ProfileNameOption,
    storage: StorageBoxOption = None,
    profile_version: ProfileVersionOption = None,
    ask_first: Annotated[
        Optional[bool],
        typer.Option(
            help=(
                "Ask whether the datafiles should be deleted, before attempting"
                " to delete the whole data root. Defaults to yes."
            ),
        ),
    ] = True,
    min_file_age: Annotated[
        Optional[int],
        typer.Option(
            help=(
                "Minimum age of files, in days, before we try to delete the data"
                " root. Defaults to no minimum age."
            )
        ),
    ] = None,
    log_file: LogFileOption = Path("clean.log"),
):
    """Delete datafiles in source data root after ingestion is complete."""
    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)

    logger.info("Extracting list of datafiles using %s profile", profile_name)
    profile = load_profile(profile_name, profile_version)
    extractor = profile.get_extractor()
    manifest = extractor.extract(source_data_path)
    # Print out all files
    datafiles = manifest.get_datafiles()
    df_paths = [
        manifest.get_data_root() / df.directory / df.filename for df in datafiles
    ]
    logger.info("The following datafiles are found in this data root.")
    for file in df_paths:
        logger.info(file)

    logger.info("Retrieving status...")
    # Check ingestion status for each file.
    timer = Timer(start=True)
    config = get_config(storage)
    insepctor = Inspector(config)
    results = [insepctor.is_datafile_verified(raw_df) for raw_df in datafiles]
    logger.info("Retrieved datafile status in %f seconds.", timer.stop())
    # Get unverified files by the index of the verification results list.
    unverified_dfs = [
        datafiles[ind] for ind, is_verified in enumerate(results) if not is_verified
    ]

    if len(unverified_dfs) > 0:
        logger.info("Datafiles pending ingestion or verification:")
        for datafile in unverified_dfs:
            logger.info(
                manifest.get_data_root() / datafile.directory / datafile.filename
            )
        logger.error(
            "Could not proceed with deleting this data root. Ingestion is not complete."
        )
        sys.exit(1)
    else:
        logger.info("Ingestion for this data root is complete.")

    if min_file_age is not None:
        logger.info("Checking file age...")
        newest_ctime = max_ctime(df_paths)
        days_since_ctime = (datetime.now() - datetime.fromtimestamp(newest_ctime)).days
        if not days_since_ctime >= min_file_age:
            logger.error(
                "Could not proceed with deleting this data root. Files were created %i days ago"
                + ", but min_file_age is %i days.",
                days_since_ctime,
                min_file_age,
            )
            sys.exit(1)
        else:
            logger.info(
                "File age is %i days, which meets min_file_age of %i days.",
                days_since_ctime,
                min_file_age,
            )
    if ask_first:
        should_delete = typer.confirm(
            "Do you want to delete all datafiles in this data root?"
        )
        if not should_delete:
            sys.exit()
    # Delete all datafiles.
    logger.info("Deleting datafiles.")
    for pth in df_paths:
        if pth.exists() and pth.is_file():
            logger.info("Deleting %s", pth)
            try:
                pth.unlink()
            except OSError:
                logger.exception("File could not be deleted: %s", pth)
                logger.error("Deleting verified files could not finish, exiting.")
                sys.exit(1)
        else:
            logger.warning(
                "File does not exist or isn't a file, skipping: %s",
                pth,
            )
    # Call profile-specific cleanup code.
    logger.info("Running profile-specific cleanup code.")
    profile.cleanup(source_data_path)
    logger.info("Finished deleting verified files.")


if __name__ == "__main__":
    app()
