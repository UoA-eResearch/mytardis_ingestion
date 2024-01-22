"""
Interface for ingestion profiles, which define the specific ingestion behaviour for
different groups and/or instruments.
"""

from abc import ABC, abstractmethod

from src.extraction.metadata_extractor import IMetadataExtractor


class IProfile(ABC):
    """Abstract base class defining the interface for an ingestion profile"""

    @abstractmethod
    def get_extractor(self) -> IMetadataExtractor:
        """Get the metadata extractor associated with this profile"""
        raise NotImplementedError("No metadata extractor has been implemented")
