"""Common functions for config"""

import logging
import sys

from pydantic import ValidationError

from src.cli.parameters import StorageBoxOption
from src.config.config import ConfigFromEnv, FilesystemStorageBoxConfig

logger = logging.getLogger(__name__)


def get_config(storage: StorageBoxOption) -> ConfigFromEnv:
    """Returns ingestion configuration parsed from the.env file.

    Args:
        storage (StorageBoxOption): Optional storage details if the ones parsed from .env
        file need to be overridden.

    Returns:
        ConfigFromEnv: Ingestion configuration.
    """
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
