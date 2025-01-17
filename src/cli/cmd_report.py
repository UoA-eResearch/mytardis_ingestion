# pylint: disable=duplicate-code, fixme
"""Module for the cleaning command."""
import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Annotated, Optional

import typer

from src.blueprints.datafile import RawDatafile
from src.cli.cmd_clean import filter_completed_dfs
from src.cli.common import (
    LogFileOption,
    LogLevelOption,
    ProfileNameOption,
    ProfileVersionOption,
    SourceDataPathArg,
    get_config,
)
from src.profiles.profile_register import load_profile
from src.utils import log_utils

logger = logging.getLogger(__name__)


def _save_data_status(
    source_path: SourceDataPathArg,
    datafiles_verified: list[RawDatafile],
    datafiles_unverified: list[RawDatafile],
) -> None:
    """Saves the status of verified and unverified data files to a CSV file."""
    # Get the current date and time
    current_datetime = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Prepare the CSV file for writing with the current date and time in the filename
    output_csv_path = source_path.parent / f"verified_datafiles_{current_datetime}.csv"

    # Combine verified and unverified datafiles into one list with status
    data_to_write = [
        {
            "filename": df.filename,
            "filepath": source_path.parent / df.filepath,
            "dataset": df.dataset,
            "ingestion_verified": verified,
        }
        for df, verified in zip(
            datafiles_verified + datafiles_unverified,
            [True] * len(datafiles_verified) + [False] * len(datafiles_unverified),
        )
    ]

    # Write all data to CSV at once
    with open(output_csv_path, "w", newline="", encoding="utf-8") as csvfile:
        fieldnames = ["filename", "filepath", "dataset", "ingestion_verified"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(data_to_write)

    logger.info("Data written to %s", output_csv_path)


# pylint: disable=too-many-locals
def report(
    source_data_path: SourceDataPathArg,
    profile_name: ProfileNameOption,
    profile_version: ProfileVersionOption = None,
    min_file_age: Annotated[
        int,
        typer.Option(
            help=(
                "Minimum age of files, in days, before they are considered for deletion."
                " Defaults to no minimum age."
            )
        ),
    ] = 0,
    log_file: LogFileOption = Path("report.log"),
    log_level: LogLevelOption = "INFO",
) -> None:
    """Report on the data source."""
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

    config = get_config()
    # Check verification status.
    verified_dfs, unverified_dfs = filter_completed_dfs(
        config, manifest.get_datafiles(), min_file_age
    )

    # save file verification status to a csv file to be stored in the research drive
    logger.info("Saving files verification status into a csv file.")
    _save_data_status(source_data_path, verified_dfs, unverified_dfs)
