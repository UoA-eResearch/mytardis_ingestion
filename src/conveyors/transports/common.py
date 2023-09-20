"""common.py - common transport-related class and interface."""
from abc import ABC
from pathlib import Path


class FailedTransferException(Exception):
    """A custom exception for transfer failures."""


class AbstractTransport(ABC):
    """Abstract class for different ways of transferring
    files.
    """

    def transfer(self, src: Path, files: list[Path]) -> None:
        """Abstract method that does the file transfer.
        Given a list of files paths, transfers to
        a configured destination.

        Raises:
            FailedTransferException: When an error happens during
            transfer.

        Args:
            src (Path): Path to the source directory
            files (list[Path]): A list of Paths of files to transfer, relative to `src`.
        """
