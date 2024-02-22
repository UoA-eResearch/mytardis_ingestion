"""Defines an interface for extracting metadata from a directory.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from src.extraction.manifest import IngestionManifest


class IMetadataExtractor(ABC):
    """Abstract Base Class defining an interface for extracting metadata from a directory
    and parsing it into an ingestible format.
    """

    @abstractmethod
    def extract(self, root_dir: Path) -> IngestionManifest:
        """Extract the data from root_dir and parse into an ingestible format."""
        raise NotImplementedError("Extraction behaviour must be defined in a subclass.")
