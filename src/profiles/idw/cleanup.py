import logging
from os import unlink
from pathlib import Path

from src.reclaimer.data_directory_cleaner import IDataRootCleaner

logger = logging.getLogger(__name__)


class IDWDataDirectoryCleaner(IDataRootCleaner):
    def cleanup(self, source_data_path: Path) -> None:
        # Delete ingestion file so scanning no longer picks up this data directory.
        logger.info("Removing ingestion file")
        if not source_data_path.exists() or not source_data_path.is_file():
            logger.warning(
                "Specified path is not a file, or ingestion file does not exist in this data root: %s",
                source_data_path,
            )
            return
        unlink(source_data_path)
