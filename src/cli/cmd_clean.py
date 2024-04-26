# pylint: disable=duplicate-code
"""Module for the cleaning command."""
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, Optional

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


def get_verified_replica(queried_df: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Given query result for a

    Args:
        raw_df (RawDatafile): The raw datafile to check.

    Returns:
        Optional[dict[str, Any]]: Returns a replica is verified, False if not.
    """
    if not queried_df["replicas"]:
        return
    replicas: list[dict[str, Any]] = queried_df["replicas"]
    # Iterate through all replicas. If one replica is verified, then
    # return it.
    for replica in replicas:
        if replica["verified"]:
            return replica
    return


def is_completed_df(
    df: RawDatafile,
    query_result: Optional[list[dict[str, Any]]],
    min_file_age: Optional[int],
) -> bool:
    pth = df.directory / df.filename
    if query_result is None or len(query_result) == 0:
        return False
    if len(query_result) > 1:
        logger.warning(
            "More than one datafile in MyTardis matched file, using the first match: %s",
            df.directory / df.filename,
        )
    replica = get_verified_replica(query_result[0])
    if replica is None:
        # No verified replica yet created.
        return False
    if min_file_age:
        # Check file age for the replica.
        vtime = datetime.fromisoformat(replica["last_verified_time"])
        days_from_vtime = (datetime.now() - vtime).days
        logger.info("%s was last verified %i days ago.", pth, days_from_vtime)
        if days_from_vtime < min_file_age:
            # If file is not old enough, consider the datafile not complete.
            return False
    # This datafile has completed ingestion. Add to list.
    return True


def filter_completed_dfs(
    config: ConfigFromEnv, datafiles: list[RawDatafile], min_file_age: Optional[int]
) -> list[dict[str, Any]]:
    logger.info("Retrieving status...")
    # Check ingestion status for each file.
    timer = Timer(start=True)
    insepctor = Inspector(config)
    results = [insepctor.query_datafile(raw_df) for raw_df in datafiles]
    unverified_dfs = []
    verified_dfs = []
    for ind, query_result in enumerate(results):
        df = datafiles[ind]
        if is_completed_df(df, query_result, min_file_age):
            verified_dfs.append(df)
        else:
            unverified_dfs.append(df)
    logger.info("Retrieved datafile status in %f seconds.", timer.stop())
    if len(unverified_dfs) > 0:
        logger.info(
            "Datafiles pending ingestion, verification or do not meet minimum file age:"
        )
        for datafile in unverified_dfs:
            logger.info(datafile.directory / datafile.filename)
        return verified_dfs
    else:
        logger.info("Ingestion for this data root is complete.")
        return verified_dfs


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
    verified_dfs = filter_completed_dfs(config, datafiles, min_file_age)
    if len(verified_dfs) != len(datafiles):
        logger.error(
            "Could not proceed with deleting this data root. Ingestion is not complete."
        )
        sys.exit(1)
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
