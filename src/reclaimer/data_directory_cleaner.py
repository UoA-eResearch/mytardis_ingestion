from abc import ABC, abstractmethod
from pathlib import Path


class IDataRootCleaner(ABC):
    """Class for data directory cleaner.

    Args:
        ABC (_type_): _description_
    """

    @abstractmethod
    def cleanup(self, source_data_path: Path) -> None:
        """Method that gets called by the profile

        Args:
            root_dir (Path): _description_
        """
        raise NotImplementedError("Cleanup method is not implemented.")


class NoopCleaner(IDataRootCleaner):
    """Default cleaner implementation that does not do anything."""

    def cleanup(self, source_data_path: Path) -> None:
        return
