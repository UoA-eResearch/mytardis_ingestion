"""Function for cleaning up IDW-specific files."""

import logging
from os import unlink
from pathlib import Path

logger = logging.getLogger(__name__)


def idw_cleanup(source_data_path: Path) -> None:
    """IDW-specific cleanup function.

    Args:
        source_data_path (Path): The source data path to clean up on.
    """
    # Delete ingestion file so scanning no longer picks up this data directory.
    logger.info("Removing ingestion file %s", source_data_path)
    if not source_data_path.exists() or not source_data_path.is_file():
        logger.warning(
            "Specified path is not a file, or ingestion file does not exist in this data root: %s",
            source_data_path,
        )
        return
    unlink(source_data_path)
