"""
Support for defining filesystem-related filters, including filters for system files.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional


@dataclass
class FileExclusionPatterns:
    """
    Patterns used to filter file paths
    """

    suffixes: Optional[list[str]]
    extension_prefixes: Optional[list[str]]


MACOS_EXCLUSION_PATTERNS = FileExclusionPatterns(
    suffixes=[
        ".DS_Store",
        "._.DS_Store",
        ".Trashes",
        ".Spotlight-V100",
        ".fseventsd",
        ".TemporaryItems",
        ".com.apple.timemachine.donotpresent",
        ".vol",
        ".AppleDouble",
        ".FileSync-lock",
        ".AppleDB",
    ],
    extension_prefixes=["._"],
)

WINDOWS_EXCLUSION_PATTERNS = FileExclusionPatterns(
    suffixes=["thumbs.db"], extension_prefixes=None
)


class PathFilterBase(ABC):
    def __init__(self) -> None:
        pass

    def __call__(self, path: Path) -> Any:
        return self.exclude(path)

    @abstractmethod
    def exclude(self, path: Path) -> bool:
        raise NotImplementedError("Cannot instantiate base class")


class NameSuffixFilter(PathFilterBase):
    def __init__(self, suffixes: list[str]) -> None:
        self._suffixes = suffixes
        super().__init__()

    def exclude(self, path: Path) -> bool:
        return any(path.name.endswith(suffix) for suffix in self._suffixes)


class ExtensionPrefixFilter(PathFilterBase):
    def __init__(self, prefixes: list[str]) -> None:
        self._prefixes = prefixes
        super().__init__()

    def exclude(self, path: Path) -> bool:
        return any(path.suffix.startswith(prefix) for prefix in self._prefixes)


class PathPatternFilter(PathFilterBase):
    def __init__(self, patterns: FileExclusionPatterns) -> None:
        self._patterns = patterns

        self._filters: list[PathFilterBase] = []

        if patterns.suffixes is not None:
            self._filters.append(NameSuffixFilter(patterns.suffixes))
        if patterns.extension_prefixes is not None:
            self._filters.append(ExtensionPrefixFilter(patterns.extension_prefixes))

        super().__init__()

    def exclude(self, path: Path) -> bool:
        return any(filt(path) for filt in self._filters)


class PathFilterSet:
    """
    A collection for holding filters to be applied to filesystem entries, to decide whether
    they are "valid" or not.
    """

    def __init__(self, filter_system_files: bool = True):
        self._filter_system_files = filter_system_files
        self._filters: list[Callable[[Path], bool]] = []

        if self._filter_system_files:
            self.add(PathPatternFilter(WINDOWS_EXCLUSION_PATTERNS))
            self.add(PathPatternFilter(MACOS_EXCLUSION_PATTERNS))

    def add(self, filter_func: Callable[[Path], bool]) -> None:
        """
        Add a filter predicate to the collection of filters
        """
        self._filters.append(filter_func)

    def exclude(self, path: Path) -> bool:
        """
        Determine whether the entry referred to by 'path' should be discarded by the filter.
        This is determined by applying the filter functions (predicates).
        """
        return any(func(path) for func in self._filters)


if __name__ == "__main__":
    filter_set = PathFilterSet(filter_system_files=True)

    print(filter_set.exclude(Path("Hello/world.txt")))
    print(filter_set.exclude(Path("Hello/world.DS_Store")))
    print(filter_set.exclude(Path("Hello/world.DS_Store.txt")))
    print(filter_set.exclude(Path("Hello/world.thumbs.db")))

    print(
        NameSuffixFilter(MACOS_EXCLUSION_PATTERNS.suffixes or []).exclude(
            Path("Hello/world.DS_Store")
        )
    )

    print(
        PathPatternFilter(MACOS_EXCLUSION_PATTERNS).exclude(
            Path("Hello/world.DS_Store")
        )
    )
