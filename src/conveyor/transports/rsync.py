"""rsync.py - module with rsync-based transport"""
import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

from src.conveyor.transports.common import AbstractTransport, FailedTransferException


def is_rsync_on_path() -> bool:
    """Tests whether rsync is available on the PATH.

    Returns:
        bool: True if rsync is found, false if not.
    """
    state = subprocess.run(["rsync", "-h"], capture_output=True, check=False)  # nosec
    return state.returncode == 0


class RsyncTransport(AbstractTransport):
    """
    A transport implementation using rsync to move files between locally-mounted
    drives.
    """

    def __init__(self, destination: Path) -> None:
        super().__init__()
        self.destination = destination
        if not is_rsync_on_path():
            raise FailedTransferException("rsync is not installed on the system.")

    def transfer(self, src: Path, files: list[Path]) -> None:
        """Concrete transfer implementation that uses rsync to move files.

        Args:
            src (Path): Path to the source directory
            files (list[Path]): A list of Paths of files to transfer, relative to `src`.

        Raises:
            FailedTransferException: Raised when rsync returns nonzero exit code.

        Returns:
            None.
        """
        with NamedTemporaryFile("r+") as list_f:
            # Write to temporary file list for rsync to sync over.
            for path in files:
                list_f.write(str(path) + "\n")
            list_f.flush()
            # Run rsync commandline to transfer the files.
            # Disable bandit's warning about subprocess.
            result = subprocess.run(  # nosec
                ["rsync", "-av", "--files-from", list_f.name, src, self.destination],
                check=False,
            )
            if result.returncode > 0:
                raise FailedTransferException("rsync return code was not 0.")
