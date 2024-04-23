"""
CLI frontend for extracting metadata, and ingesting it with the data into MyTardis.
"""

import logging
import sys
from pathlib import Path
from typing import Optional, TypeAlias

import rich.progress
import typer
from pydantic import ValidationError

# from rich.progress import Progress
from typing_extensions import Annotated

from src.config.config import ConfigFromEnv, FilesystemStorageBoxConfig
from src.extraction.manifest import IngestionManifest
from src.ingestion_factory.factory import IngestionFactory
from src.profiles.profile_register import load_profile
from src.utils import log_utils, notifiers
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


class RichProgressAdapter:
    """Displays progress updates from a notifier in a rich progress bar."""

    def __init__(
        self, notifier: notifiers.ProgressUpdater, progress_bar: rich.progress.Progress
    ):
        self._task_ids: dict[str, rich.progress.TaskID] = {}
        self._notifier = notifier
        self._progress_bar = progress_bar

        def do_init(name: str, total: Optional[int] = None) -> None:
            self._task_ids[name] = self._progress_bar.add_task(name, total=total)

        self._notifier.on_init(do_init)

        def handle_update(name: str, increment: int) -> None:
            self._progress_bar.update(self._task_ids[name], advance=increment)

        self._notifier.on_update(handle_update)


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
    try:
        config = ConfigFromEnv()
        if storage is not None:
            # Create storagebox config based on passed in argument.
            store = FilesystemStorageBoxConfig(
                storage_name=storage[0], target_root_dir=storage[1]
            )
            config.storage = store
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

    logging.info("Loading pre-extracted metadata from %s", manifest_dir)

    # with Progress() as progress:
    # notifier = notifiers.ProgressUpdater()
    # progress_displayer = RichProgressAdapter(notifier, progress)
    manifest = IngestionManifest.deserialize(manifest_dir, notifier=None)

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
    try:
        config = ConfigFromEnv()
        if storage is not None:
            # Create storagebox config based on passed in argument.
            store = FilesystemStorageBoxConfig(
                storage_name=storage[0], target_root_dir=storage[1]
            )
            config.storage = store
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


if __name__ == "__main__":
    app()
