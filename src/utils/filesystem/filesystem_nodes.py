"""
Helpers for navigating the filesystem, querying layout and finding entries
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Iterator, Tuple, TypeAlias, TypeVar

T = TypeVar("T")
Predicate: TypeAlias = Callable[[T], bool]


def collect_children(
    directory: DirectoryNode,
) -> Tuple[list[FileNode], list[DirectoryNode]]:
    """Gather lists of all files and directories in the directory
    referenced by 'directory'

    Args:
        directory (DirectoryNode): The directory whose entries are to be collected

    Returns:
        Tuple[list[FileNode], list[DirectoryNode]]: Lists of files/directories in 'directory'
    """
    files: list[FileNode] = []
    directories: list[DirectoryNode] = []

    for child in directory.path().iterdir():
        if child.is_file():
            files.append(
                FileNode(
                    child,
                    parent=directory,
                    check_exists=False,
                )
            )
        elif child.is_dir():
            directories.append(
                DirectoryNode(
                    child,
                    parent=directory,
                    check_exists=False,
                )
            )

    return (files, directories)


class FileNode:
    """Represents a file entry in the filesystem. Mainly useful in conjunction with
    _DirectoryNode_
    """

    def __init__(
        self,
        path: Path,
        parent: DirectoryNode | None = None,
        check_exists: bool = True,
    ) -> None:
        self._path = path
        self._parent: DirectoryNode | None = parent
        self._stat_info: os.stat_result | None = None

        if check_exists and not path.is_file():
            raise FileNotFoundError(f"{path} is not a valid file")

    def __eq__(self, other: object) -> bool:
        if isinstance(other, FileNode):
            return self.path() == other.path()
        raise NotImplementedError("Invalid equality comparison")

    def name(self) -> str:
        """
        Get the file name
        """
        return self._path.name

    def extension(self) -> str:
        """Get the file extension"""
        return self._path.suffix

    def parent(self) -> DirectoryNode:
        """Get this file's parent directory"""
        if self._parent is None:
            # Assuming here that a file must always have a parent directory
            self._parent = DirectoryNode(self._path.parent)

        return self._parent

    def path(self) -> Path:
        """Get the path to the file

        Returns:
            Path: the path
        """
        return self._path

    def stat(self) -> os.stat_result:
        """Retrieve file information from a stat() call"""
        if self._stat_info is None:
            self._stat_info = self.path().stat()
        return self._stat_info


class DirectoryNode:
    """Represents a directory entry in the filesystem, and provides operations for
    querying and traversing it.
    """

    def __init__(
        self,
        path: Path,
        parent: DirectoryNode | None = None,
        check_exists: bool = True,
    ):
        self._path = path
        self._parent = parent
        self._dirs: list[DirectoryNode] | None = None
        self._files: list[FileNode] | None = None

        if check_exists and not path.is_dir():
            raise NotADirectoryError(f"'{path}' is not a valid directory")

    def name(self) -> str:
        """Get the name of the directory"""
        return self._path.name

    def path(self) -> Path:
        """Get the path of the directory"""
        return self._path

    def is_root(self) -> bool:
        """Check whether this is a root directory (i.e. it has no parent)"""
        return self._path.parent == self._path

    def parent(self) -> DirectoryNode:
        """Get this directory's parent directory.

        Raises NotADirectoryError if this directory has no parent.
        """
        if self._parent is None:
            if self.is_root():
                raise NotADirectoryError(f"Directory '{self._path}' has no parent")
            self._parent = DirectoryNode(self._path.parent)

        return self._parent

    def file(self, name: str) -> FileNode:
        """Get a _FileNode_ representing a file in this directory named _name_.

        Raises FileNotFoundError if no such file exists
        """
        for file in self.files():
            if file.path().name == name:
                return file
        raise FileNotFoundError(f"Directory '{self.path()}' contains no file '{name}'")

    def dir(self, name: str) -> DirectoryNode:
        """Get a _DirectoryNode_ representing a directory in this directory named _name_.

        Raises NotADirectoryError if no such directory exists
        """
        for directory in self.directories():
            if directory.path().name == name:
                return directory
        raise NotADirectoryError(
            f"Directory '{self.path()}' contains no directory '{name}'"
        )

    def files(self) -> list[FileNode]:
        """Get a list of all the files in this directory"""
        if self._files is None:
            self._files, self._dirs = collect_children(self)
        return self._files

    def directories(self) -> list[DirectoryNode]:
        """Get a list of all the directories in this directory"""
        if self._dirs is None:
            self._files, self._dirs = collect_children(self)
        return self._dirs

    def iter_files(self, recursive: bool = False) -> Iterator[FileNode]:
        """Get an iterator for all files under this directory.

        If recursive=True, it will yield files in subdirectories too"""
        for file in self.files():
            yield file

        if recursive:
            for directory in self.directories():
                yield from directory.iter_files(recursive=True)

    def iter_dirs(self, recursive: bool = False) -> Iterator[DirectoryNode]:
        """Get an iterator for all directories under this directory.

        If recursive=True, it will yield subdirectories too"""
        for directory in self.directories():
            yield directory

        if recursive:
            for directory in self.directories():
                yield from directory.iter_dirs(recursive=True)

    def has_file(self, name: str) -> bool:
        """ "Check whether there is a file named _name_ in this directory"""
        return (self._path / name).is_file()

    def has_dir(self, name: str) -> bool:
        """ "Check whether there is a directory named _name_ in this directory"""
        return (self._path / name).is_dir()

    def empty(self) -> bool:
        """ "Check whether this directory contains any files or directories"""
        return len(self.files()) == 0 and len(self.directories()) == 0

    def visit_files(
        self, func: Callable[[FileNode], None], recursive: bool = False
    ) -> None:
        """Iterate over the files, calling _func_ on each one"""
        for file in self.iter_files(recursive=recursive):
            func(file)

    def visit_directories(
        self, func: Callable[[DirectoryNode], None], recursive: bool = False
    ) -> None:
        """Iterate over the directories, calling _func_ on each one"""
        for directory in self.iter_dirs(recursive=recursive):
            func(directory)

    def find_files(
        self, keep_file: Predicate[FileNode], recursive: bool = False
    ) -> list[FileNode]:
        """Produce a list of all files satisfying the predicate _keep_file_"""
        return [f for f in self.iter_files(recursive) if keep_file(f)]

    def find_dirs(
        self, keep_dir: Predicate[DirectoryNode], recursive: bool = False
    ) -> list[DirectoryNode]:
        """Produce a list of all directories satisfying the predicate _keep_dir_"""
        return [d for d in self.iter_dirs(recursive) if keep_dir(d)]
