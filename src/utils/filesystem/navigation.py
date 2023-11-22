"""
Helpers for navigating the filesystem, querying layout and finding entries
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Iterator, Tuple, TypeAlias, TypeVar

T = TypeVar("T")
Predicate: TypeAlias = Callable[[T], bool]


def collect_children(
    directory: DirectoryNode,
) -> Tuple[list[FileNode], list[DirectoryNode]]:
    files: list[FileNode] = []
    directories: list[DirectoryNode] = []

    for child in directory.path().iterdir():
        if child.is_file():
            files.append(FileNode(child, parent=directory, check_exists=False))
        elif child.is_dir():
            directories.append(DirectoryNode(child, check_exists=False))

    return (files, directories)


class FileNode:
    def __init__(
        self,
        path: Path,
        parent: DirectoryNode | None = None,
        check_exists: bool = True,
    ) -> None:
        self._path = path
        self._parent: DirectoryNode | None = parent

        if check_exists and not path.is_file():
            raise FileNotFoundError(f"{path} is not a valid file")

    def name(self) -> str:
        return self._path.name

    def extension(self) -> str:
        return self._path.suffix

    def parent(self) -> DirectoryNode:
        if self._parent is None:
            # Assuming here that a file must always have a parent directory
            self._parent = DirectoryNode(self._path.parent)

        return self._parent

    def path(self) -> Path:
        return self._path


class DirectoryNode:
    def __init__(self, path: Path, check_exists: bool = True):
        self._path = path
        self._parent: DirectoryNode | None = None
        self._dirs: list[DirectoryNode] | None = None
        self._files: list[FileNode] | None = None

        if check_exists and not path.is_dir():
            raise NotADirectoryError(f"'{path}' is not a valid directory")

    def name(self) -> str:
        return self._path.name

    def path(self) -> Path:
        return self._path

    def is_root(self) -> bool:
        return self._path.parent == self._path

    def parent(self) -> DirectoryNode:
        if self._parent is None:
            if self.is_root():
                raise NotADirectoryError(f"Directory '{self._path}' has no parent")
            self._parent = DirectoryNode(self._path.parent)

        return self._parent

    def file(self, name: str) -> FileNode:
        for file in self.files():
            if file.path().name == name:
                return file
        raise ValueError(f"Directory '{self.path()}' contains no file '{name}'")

    def dir(self, name: str) -> DirectoryNode:
        for directory in self.directories():
            if directory.path().name == name:
                return directory
        raise ValueError(f"Directory '{self.path()}' contains no directory '{name}'")

    def files(self) -> list[FileNode]:
        if self._files is None:
            self._files, self._dirs = collect_children(self)
        return self._files

    def directories(self) -> list[DirectoryNode]:
        if self._dirs is None:
            self._files, self._dirs = collect_children(self)
        return self._dirs

    def iter_files(self, recursive: bool = False) -> Iterator[FileNode]:
        for file in self.files():
            yield file

        if recursive:
            for directory in self.directories():
                yield from directory.iter_files(recursive=True)

    def iter_dirs(self, recursive: bool = False) -> Iterator[DirectoryNode]:
        for directory in self.directories():
            yield directory

        if recursive:
            for directory in self.directories():
                yield from directory.iter_dirs(recursive=True)

    def has_file(self, name: str) -> bool:
        return (self._path / name).is_file()

    def has_dir(self, name: str) -> bool:
        return (self._path / name).is_dir()

    def non_empty(self) -> bool:
        return len(self.files()) > 0 or len(self.directories()) > 0

    def visit_files(
        self, func: Callable[[FileNode], None], recursive: bool = False
    ) -> None:
        for file in self.iter_files(recursive=recursive):
            func(file)

    def visit_directories(
        self, func: Callable[[DirectoryNode], None], recursive: bool = False
    ) -> None:
        for directory in self.iter_dirs(recursive=recursive):
            func(directory)
