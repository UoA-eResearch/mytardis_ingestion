"""
Interface for ingestion profiles, which define the specific ingestion behaviour for
different groups and/or instruments.
"""

from abc import ABC, abstractmethod
from pathlib import Path

from src.extraction.metadata_extractor import IMetadataExtractor


class IProfile(ABC):
    """Abstract base class defining the interface for an ingestion profile"""

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the name of this profile"""
        raise NotImplementedError("Concrete implementations must specify their name")

    @abstractmethod
    def get_extractor(self) -> IMetadataExtractor:
        """Get the metadata extractor associated with this profile"""
        raise NotImplementedError("No metadata extractor has been implemented")

    def cleanup(self, source_data_path: Path) -> None:
        """Optional method for performing profile-specific cleanup steps in the
        source_data_path. This method is called after datafiles are deleted by
        the clean command."""
