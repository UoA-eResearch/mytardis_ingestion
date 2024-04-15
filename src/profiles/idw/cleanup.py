import logging
from os import unlink
from pathlib import Path

from src.reclaimer.data_directory_cleaner import IDataRootCleaner

logger = logging.getLogger(__name__)


class IDWDataDirectoryCleaner(IDataRootCleaner):
    def cleanup(self, source_data_path: Path) -> None:
        # Delete ingestion file so scanning no longer picks up this data directory.
        logger.info("Removing ingestion file")
        ingestion_file_path = source_data_path / "ingestion.yaml"
        if not ingestion_file_path.exists() or not ingestion_file_path.is_file():
            logger.warning(
                "Speciified path is not a file, or ingestion file does not exist in this data root."
            )
            return
        unlink(source_data_path)
