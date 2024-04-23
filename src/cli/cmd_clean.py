# pylint: disable=duplicate-code
"""Module for the cleaning command."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer

from src.blueprints.datafile import RawDatafile
from src.cli.common import (
    LogFileOption,
    ProfileNameOption,
    ProfileVersionOption,
    SourceDataPathArg,
    StorageBoxOption,
    get_config,
)
from src.config.config import ConfigFromEnv
from src.inspector.inspector import Inspector
from src.profiles.profile_register import load_profile
from src.utils import log_utils
from src.utils.filesystem.ctime import max_ctime
from src.utils.timing import Timer

logger = logging.getLogger(__name__)


def _check_verified_status(config: ConfigFromEnv, datafiles: list[RawDatafile]) -> None:
    logger.info("Retrieving status...")
    # Check ingestion status for each file.
    timer = Timer(start=True)
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
            logger.info(datafile.directory / datafile.filename)
        logger.error(
            "Could not proceed with deleting this data root. Ingestion is not complete."
        )
        sys.exit(1)
    else:
        logger.info("Ingestion for this data root is complete.")


def _check_file_age(df_paths: list[Path], min_file_age: int) -> None:
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


def _delete_datafiles(df_paths: list[Path]) -> None:
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
) -> None:
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

    config = get_config(storage)
    # Check verification status.
    _check_verified_status(config, datafiles)

    if min_file_age is not None:
        _check_file_age(df_paths, min_file_age)
    if ask_first:
        should_delete = typer.confirm(
            "Do you want to delete all datafiles in this data root?"
        )
        if not should_delete:
            sys.exit()
    # Delete all datafiles.
    _delete_datafiles(df_paths)
    # Call profile-specific cleanup code.
    logger.info("Running profile-specific cleanup code.")
    profile.cleanup(source_data_path)
    logger.info("Finished deleting verified files.")
