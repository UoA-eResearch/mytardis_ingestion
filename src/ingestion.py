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
from src.crucible.crucible import Crucible
from src.extraction.manifest import IngestionManifest
from src.ingestion_factory.factory import IngestionFactory
from src.mytardis_client.mt_rest import MyTardisRESTFactory
from src.overseers.overseer import Overseer
from src.profiles.profile_register import load_profile
from src.reclaimer.reclaimer import Reclaimer
from src.smelters.smelter import Smelter
from src.utils import log_utils
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
    timer = Timer(start=True)

    config = get_config(storage)

    profile = load_profile(profile_name, profile_version)
    extractor = profile.get_extractor()
    manifest = extractor.extract(source_data_path)
    # Print out all files
    files_found = manifest.get_datafiles()
    logger.info("The following datafiles are found in this data root.")
    for file in files_found:
        logger.info(manifest.get_data_root() / file.directory / file.filename)

    # Create ingestion machinery to prepare datafiles so we can check
    # verification status.
    mt_rest = MyTardisRESTFactory(config.auth, config.connection)
    overseer = Overseer(mt_rest)
    smelter = Smelter(
        overseer=overseer, general=config.general, default_schema=config.default_schema
    )
    crucible = Crucible(overseer)
    # Check ingestion status for each file.
    logger.info("Retrieving status...")
    reclaimer = Reclaimer(overseer, smelter, crucible)
    results = reclaimer.fetch_datafiles_status(manifest.get_datafiles())
    logger.info("Retrieved datafile status in %f seconds.", timer.stop())
    unverified_dfs = [result.file for result in results if not result.is_verified]

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
        newest_ctime = 0
        for df in manifest.get_datafiles():
            pth = manifest.get_data_root() / df.directory / df.filename
            if pth.exists() and pth.is_file():
                # Find the newest file's age.
                ctime = pth.lstat().st_ctime
                if ctime > newest_ctime:
                    newest_ctime = ctime
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
                "File age is %i days, which exceeds min_file_age %i days.",
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
    logger.info("Deleting verified files.")
    for df in manifest.get_datafiles():
        pth = manifest.get_data_root() / df.directory / df.filename
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
    cleaner = profile.get_cleanup()
    cleaner.cleanup(source_data_path)
    logger.info("Finished deleting verified files.")


if __name__ == "__main__":
    app()
