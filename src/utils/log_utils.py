"""
Helpers for configuring and formatting logging outputs
"""

import logging
import sys
from typing import Optional


def init_logging(
    file_name: Optional[str] = None, level: int | str = logging.DEBUG
) -> None:
    """
    Configure a basic default logging setup. Logs to the console, and optionally
    to a file, if a filename is passed.
    """
    root = logging.getLogger()
    root.setLevel(level)

    log_format = "[%(asctime)s %(levelname)s]: %(message)s"

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter(log_format))
    root.addHandler(console_handler)

    if file_name:
        file_handler = logging.FileHandler(filename=file_name, mode="w")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter(log_format))
        root.addHandler(file_handler)
