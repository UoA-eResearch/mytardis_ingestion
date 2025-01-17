# pylint: disable=duplicate-code
"""Module for the cleaning command."""
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional, Tuple

import typer

from src.blueprints.datafile import RawDatafile
from src.cli.common import (
    LogFileOption,
    LogLevelOption,
    ProfileNameOption,
    ProfileVersionOption,
    SourceDataPathArg,
    StorageBoxOption,
    get_config,
)
from src.config.config import ConfigFromEnv
from src.inspector.inspector import Inspector
from src.mytardis_client.response_data import IngestedDatafile, Replica
from src.profiles.profile_register import load_profile
from src.utils import log_utils
from src.utils.timing import Timer

logger = logging.getLogger(__name__)


def _get_oldest_replica(df: IngestedDatafile) -> Optional[Replica]:
    """Given a Datafile, return the oldest verified replica."""
    replicas = [replica for replica in df.replicas if replica.verified]
    if len(replicas) == 0:
        return None
    return sorted(
        replicas,
        key=lambda replica: datetime.fromisoformat(
            # If there's no verified time, assume today.
            # This situation is not possible, because a verified replica
            # will have a verification time.
            replica.last_verified_time
            or datetime.today().isoformat()
        ),
    )[0]


def is_complete_df(df: IngestedDatafile, min_file_age: int = 0) -> bool:
    """Returns whether the datafile is complete - metadata ingested, file transferred,
    and at least one verified replica that exceeds minimum file page."""
    oldest_replica = _get_oldest_replica(df)
    if oldest_replica is None:
        # If there are no verified replicas, return false.
        return False
    vtime = datetime.fromisoformat(
        oldest_replica.last_verified_time or datetime.today().isoformat()
    )
    days_from_vtime = (datetime.now() - vtime).days
    # Returns whether the complete replica exceeds minimum file age we want. Defaults to 0,
    # which means any verified replica will do.
    return days_from_vtime >= min_file_age


def filter_completed_dfs(
    config: ConfigFromEnv, datafiles: list[RawDatafile], min_file_age: int
) -> Tuple[list[RawDatafile], list[RawDatafile]]:
    """Inspects through a list of datafiles and returns two lists: one with the datafiles
    that have been ingested and verified, and another with the datafiles that have not been
    ingested or verified."""
    # Query the API about the datafiles.
    inspector = Inspector(config)
    unverified_dfs = []
    verified_dfs = []
    for df in datafiles:
        # Checks whether the datafile has completed ingestion, and group into two lists.
        query_result = inspector.query_datafile(df)
        if query_result is None:
            # Could not find datafile.
            unverified_dfs.append(df)
            continue
        if len(query_result) > 1:
            logger.warning(
                "More than one datafile in MyTardis matched file, using the first match: %s",
                df.filepath,
            )
        if is_complete_df(query_result[0], min_file_age):
            verified_dfs.append(df)
        else:
            unverified_dfs.append(df)
    return verified_dfs, unverified_dfs


def _delete_datafiles(df_paths: list[Path]) -> None:
    """Delete a list of files."""
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


# pylint: disable=too-many-locals
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
                " to delete the whole data source. Defaults to yes."
            ),
        ),
    ] = True,
    min_file_age: Annotated[
        int,
        typer.Option(
            help=(
                "Minimum age of files, in days, before we try to delete the data"
                " root. Defaults to no minimum age."
            )
        ),
    ] = 0,
    log_file: LogFileOption = Path("clean.log"),
    log_level: LogLevelOption = "INFO",
) -> None:
    """Delete datafiles in source data source after ingestion is complete."""
    log_utils.init_logging(file_name=str(log_file), level=log_level)

    logger.info("Extracting list of datafiles using %s profile", profile_name)
    profile = load_profile(profile_name, profile_version)
    manifest = profile.get_extractor().extract(source_data_path)
    # Print out all files
    df_paths = [
        manifest.get_data_root() / df.filepath for df in manifest.get_datafiles()
    ]
    logger.info("The following datafiles are found in this data source.")
    for file in df_paths:
        logger.info(file)

    config = get_config(storage)
    logger.info("Retrieving status...")
    # Check ingestion status for each file.
    timer = Timer(start=True)
    # Check verification status.
    verified_dfs, unverified_dfs = filter_completed_dfs(
        config, manifest.get_datafiles(), min_file_age
    )
    logger.info("Retrieved datafile status in %f seconds.", timer.stop())
    if len(unverified_dfs) > 0:
        # If there were unverified datafiles, print them out.
        logger.info(
            "Datafiles pending ingestion, verification or do not meet minimum file age:"
        )
        for datafile in unverified_dfs:
            logger.info(datafile.filepath)
    else:
        logger.info("Ingestion for this data source is complete.")

    verified_df_paths = [manifest.get_data_root() / df.filepath for df in verified_dfs]

    if len(verified_dfs) != len(manifest.get_datafiles()):
        logger.error(
            "Could not proceed with deleting this data source. Ingestion is not complete."
        )
        sys.exit(1)
    if ask_first:
        should_delete = typer.confirm(
            "Do you want to delete all datafiles in this data source?"
        )
        if not should_delete:
            sys.exit()

    # Delete all datafiles.
    _delete_datafiles(verified_df_paths)
    # Call profile-specific cleanup code.
    logger.info("Running profile-specific cleanup code.")
    profile.cleanup(source_data_path)
    logger.info("Finished deleting verified files.")
