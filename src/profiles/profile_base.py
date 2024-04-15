"""
Interface for ingestion profiles, which define the specific ingestion behaviour for
different groups and/or instruments.
"""

from abc import ABC, abstractmethod

from src.extraction.metadata_extractor import IMetadataExtractor
from src.reclaimer.data_directory_cleaner import IDataRootCleaner, NoopCleaner


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

    def get_cleanup(self) -> IDataRootCleaner:
        """
        Perform profile-specific cleanup task, after a data directory has been fully ingested.
        """
        return NoopCleaner()
