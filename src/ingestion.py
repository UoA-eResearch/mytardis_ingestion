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

from src.blueprints.datafile import Datafile
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
# SHARED ARGUMENT DEFINITIONS
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
    source_data_path: SourceDataPathArg,
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
    storage_name: Annotated[
        str, typer.Argument(help="Name of the staging storagebox.")
    ],
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


CleanVerifiedFilesOpt = Annotated[
    Optional[bool],
    typer.Option(
        help="Delete files which have been verified in MyTardis.",
    ),
]


@app.command()
def clean(
    source_data_path: SourceDataPathArg,
    storage_name: Annotated[
        str, typer.Argument(help="Name of the staging storagebox.")
    ],
    storage_dir: Annotated[
        Path,
        typer.Argument(help="Directory where the extracted data will be stored"),
    ],
    profile_name: ProfileNameArg,
    profile_version: ProfileVersionArg = None,
    clean_verified_files: CleanVerifiedFilesOpt = False,
    log_file: LogFileArg = Path("filestatus.log"),
):

    log_utils.init_logging(file_name=str(log_file), level=logging.DEBUG)
    timer = Timer(start=True)

    try:
        config = ConfigFromEnv(
            # Create storagebox config based on passed in argument.
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

    profile = load_profile(profile_name, profile_version)
    extractor = profile.get_extractor()
    manifest = extractor.extract(source_data_path)
    # Create ingestion machinery to prepare datafiles so we can check
    # verification status.
    mt_rest = MyTardisRESTFactory(config.auth, config.connection)
    overseer = Overseer(mt_rest)
    smelter = Smelter(
        overseer=overseer, general=config.general, default_schema=config.default_schema
    )
    crucible = Crucible(overseer)
    # Check ingestion status for each file.
    reclaimer = Reclaimer(storage_name, overseer, smelter, crucible)
    result = reclaimer.get_ingestion_status(manifest.get_datafiles())
    logger.info("Retrieved file status in %f seconds.", timer.stop())
    if len(result.unverified_files) > 0:
        logger.info("Datafiles pending verification:")
        for df in result.unverified_files:
            logger.info(df.directory / df.filename)
    if clean_verified_files:
        num_verified = len(result.verified_files)
        num_unverified = len(result.unverified_files)
        num_dfs_is_same_as_raw = (num_verified + num_unverified) == len(
            manifest.get_datafiles()
        )
        if num_unverified > 0 or not num_dfs_is_same_as_raw:
            logger.error(
                "Could not delete datafiles in this data root,"
                + "not all files could be verified."
            )
            sys.exit(1)
        logger.info("Deleting verified files.")
        for df in result.verified_files:
            pth = manifest.get_data_root() / df.directory / df.filename
            if pth.exists() and pth.is_file():
                logger.info("Deleting %s", pth)
                pth.unlink()
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
