# pylint: disable=missing-docstring
# mypy: disable-error-code="no-untyped-def"

from pathlib import Path

import pytest
from pyfakefs.fake_filesystem import FakeFilesystem

from src.utils.filesystem.filesystem_nodes import DirectoryNode, FileNode


@pytest.fixture(name="_fake_filesystem")
def fixture_fake_filesystem(fs: FakeFilesystem):
    fs.create_file("/test/a.txt")
    fs.create_file("/test/b.jpg")
    fs.create_file("/test/foo/b.txt")
    fs.create_file("/test/foo/c.png")
    fs.create_file("/test/bar/d.pdf")
    fs.create_file("/test/bar/e.py")
    fs.create_file("/test/foo/baz/f.mov")
    fs.create_file("/test/foo/baz/g.json")
    fs.create_dir("/test/foo/empty")

    yield fs


def test_file_node(_fake_filesystem: FakeFilesystem):
    # Invalid usage, but it should not check and should not throw
    _ = FileNode(Path("/not/a/file.txt"), check_exists=False)

    with pytest.raises(FileNotFoundError):
        _ = FileNode(Path("/not/a/file.txt"), check_exists=True)

    file_path = Path("/test/foo/baz/g.json")
    file_node = FileNode(file_path)

    assert file_node.name() == "g.json"
    assert file_node.extension() == ".json"
    assert file_node.path() == file_path

    parent_dir = file_node.parent()

    assert parent_dir.name() == "baz"
    assert parent_dir.path() == Path("/test/foo/baz")


def test_directory_node_init(_fake_filesystem: FakeFilesystem):
    _ = DirectoryNode(Path("/"), parent=None, check_exists=True)

    # Should not throw if not checked
    _ = DirectoryNode(Path("/does/not/exist"), parent=None, check_exists=False)

    with pytest.raises(NotADirectoryError):
        _ = DirectoryNode(Path("/does/not/exist"), parent=None, check_exists=True)


def test_directory_node_info(_fake_filesystem: FakeFilesystem):
    foo_dir = DirectoryNode(Path("/test/foo"), parent=None, check_exists=True)

    assert foo_dir.name() == "foo"
    assert foo_dir.path() == Path("/test/foo")
    assert not foo_dir.empty()
    assert not foo_dir.is_root()

    root_dir = DirectoryNode(Path("/"), parent=None, check_exists=True)
    assert root_dir.is_root()

    empty_dir = DirectoryNode(Path("/test/foo/empty"), parent=None, check_exists=True)
    assert empty_dir.empty()


def test_directory_node_parents(_fake_filesystem: FakeFilesystem):
    root = DirectoryNode(Path("/"), parent=None, check_exists=True)

    with pytest.raises(NotADirectoryError):
        _ = root.parent()

    foo_dir = DirectoryNode(Path("/test/foo"))
    assert foo_dir.parent().path() == Path("/test")

    baz_dir = DirectoryNode(Path("/test/foo/baz"))
    assert baz_dir.parent().path() == foo_dir.path()


def test_directory_node_query_files(_fake_filesystem: FakeFilesystem):
    test_dir = DirectoryNode(Path("/test"), parent=None, check_exists=True)

    assert test_dir.has_file("a.txt")
    assert test_dir.has_file("b.jpg")
    assert not test_dir.has_file("non_existent.txt")

    file_a = test_dir.file("a.txt")
    assert file_a.name() == "a.txt"

    with pytest.raises(FileNotFoundError):
        _ = test_dir.file("non_existent.txt")

    files = test_dir.files()
    assert len(files) == 2
    assert files[0].name() == "a.txt"
    assert files[1].name() == "b.jpg"

    iter_files = list(test_dir.iter_files(recursive=False))
    assert len(iter_files) == 2
    assert iter_files[0].name() == "a.txt"
    assert iter_files[1].name() == "b.jpg"

    iter_files_recursive = list(test_dir.iter_files(recursive=True))
    assert len(iter_files_recursive) == 8
    assert iter_files_recursive[0].path() == Path("/test/a.txt")
    assert iter_files_recursive[1].path() == Path("/test/b.jpg")
    assert iter_files_recursive[2].path() == Path("/test/bar/d.pdf")
    assert iter_files_recursive[3].path() == Path("/test/bar/e.py")
    assert iter_files_recursive[4].path() == Path("/test/foo/b.txt")
    assert iter_files_recursive[5].path() == Path("/test/foo/c.png")
    assert iter_files_recursive[6].path() == Path("/test/foo/baz/f.mov")
    assert iter_files_recursive[7].path() == Path("/test/foo/baz/g.json")

    empty_dir = DirectoryNode(Path("/test/foo/empty"))
    assert len(empty_dir.files()) == 0


def test_directory_node_query_directories(_fake_filesystem: FakeFilesystem):
    test_dir = DirectoryNode(Path("/test"), parent=None, check_exists=True)

    assert test_dir.has_dir("foo")
    assert test_dir.has_dir("bar")

    dir_foo = test_dir.dir("foo")
    assert dir_foo.name() == "foo"

    with pytest.raises(NotADirectoryError):
        _ = test_dir.dir("non_existent")

    dirs = test_dir.directories()
    assert len(dirs) == 2
    assert dirs[0].name() == "bar"
    assert dirs[1].name() == "foo"

    iter_dirs = list(test_dir.iter_dirs(recursive=False))
    assert len(iter_dirs) == 2
    assert iter_dirs[0].name() == "bar"
    assert iter_dirs[1].name() == "foo"

    iter_dirs_recursive = list(test_dir.iter_dirs(recursive=True))
    assert len(iter_dirs_recursive) == 4
    assert iter_dirs_recursive[0].path() == Path("/test/bar")
    assert iter_dirs_recursive[1].path() == Path("/test/foo")
    assert iter_dirs_recursive[2].path() == Path("/test/foo/baz")
    assert iter_dirs_recursive[3].path() == Path("/test/foo/empty")

    empty_dir = DirectoryNode(Path("/test/foo/empty"))
    assert len(empty_dir.directories()) == 0


def test_directory_node_visit_entries(_fake_filesystem: FakeFilesystem):
    test_dir = DirectoryNode(Path("/test"))

    arg_paths: list[Path] = []

    def stash_path(file_node: FileNode | DirectoryNode) -> None:
        arg_paths.append(file_node.path())

    test_dir.visit_files(stash_path, recursive=False)

    assert arg_paths == [
        Path("/test/a.txt"),
        Path("/test/b.jpg"),
    ]

    arg_paths.clear()

    test_dir.visit_files(stash_path, recursive=True)
    assert arg_paths == [
        Path("/test/a.txt"),
        Path("/test/b.jpg"),
        Path("/test/bar/d.pdf"),
        Path("/test/bar/e.py"),
        Path("/test/foo/b.txt"),
        Path("/test/foo/c.png"),
        Path("/test/foo/baz/f.mov"),
        Path("/test/foo/baz/g.json"),
    ]

    arg_paths.clear()

    test_dir.visit_directories(stash_path, recursive=False)
    assert arg_paths == [
        Path("/test/bar"),
        Path("/test/foo"),
    ]

    arg_paths.clear()

    test_dir.visit_directories(stash_path, recursive=True)
    assert arg_paths == [
        Path("/test/bar"),
        Path("/test/foo"),
        Path("/test/foo/baz"),
        Path("/test/foo/empty"),
    ]


def test_directory_node_find_entries(_fake_filesystem: FakeFilesystem):
    test_dir = DirectoryNode(Path("/test"))

    found_files = test_dir.find_files(lambda f: f.extension() == ".txt", recursive=True)

    found_file_paths = [f.path() for f in found_files]
    assert found_file_paths == [
        Path("/test/a.txt"),
        Path("/test/foo/b.txt"),
    ]

    found_dirs = test_dir.find_dirs(
        lambda d: d.parent().name() == "foo", recursive=True
    )

    found_dir_paths = [d.path() for d in found_dirs]
    assert found_dir_paths == [
        Path("/test/foo/baz"),
        Path("/test/foo/empty"),
    ]
