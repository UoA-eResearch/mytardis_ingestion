# pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring

import json
import logging
import shutil
from pathlib import Path

import responses
from pytest import fixture

from src.beneficiations import beneficiation_consts as bc
from src.extraction_output_manager.output_manager import OutputManager
from src.profiles import profile_consts as pc

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


@fixture
def output_manager_generation():
    return OutputManager()


@fixture
def get_abi_profile():
    return "abi_music"


@fixture
def get_bad_beneficiation_format():
    return "bad_format"


@fixture
def tmpdir_metadata():
    tempdir = None
    dir1 = None
    dir2 = None

    base_pth = Path(".")

    tempdir = base_pth / Path("tempdir")
    if not tempdir.exists():
        tempdir.mkdir()

    dir1name = Path("dir1")
    dir1 = tempdir / dir1name
    if not dir1.exists():
        dir1.mkdir()

    dir2name = Path("dir2")
    dir2 = dir1 / dir2name
    if not dir2.exists():
        dir2.mkdir()

    dir3rawname = Path("dir3-Raw")
    dir3raw = dir2 / dir3rawname
    if not dir3raw.exists():
        dir3raw.mkdir()

    dir3deconvname = Path("dir3-Deconv")
    dir3deconv = dir2 / dir3deconvname
    if not dir3deconv.exists():
        dir3deconv.mkdir()

    dirrawname = Path("dir-Raw")
    dirraw = dir2 / dirrawname
    if not dirraw.exists():
        dirraw.mkdir()

    dir3name = Path("dir3")

    dummy_data_raw = {
        "Basename": {
            "Project": str(dir1name),
            "Sample": str(dir2name),
            "Sequence": str(dir3name),
        },
        "Description": "Dummy Description",
        "SequenceID": 123456789,
        "Camera Settings": {"Fan": False, "Pixel Correction": False, "Cooling": False},
    }

    dummy_data_deconv = {
        "Basename": {
            "Project": str(dir1name),
            "Sample": str(dir2name),
            "Sequence": str(dir3name),
        },
        "Description": "Dummy Description",
        "SequenceID": 123456789,
        "Camera Settings": {"Fan": False, "Pixel Correction": False, "Cooling": False},
    }

    dummy_data_bad = {
        "Basename": {
            "Project": str(dir1name),
            "Sample": str(dir2name),
            "Sequence": str(dir3name),
        },
        "Description": "Dummy Description",
        "SequenceID": 123456789,
        "Camera Settings": {"Fan": False, "Pixel Correction": False, "Cooling": False},
    }

    json_file_suffix = ".json"
    fpRaw = dir3raw / Path(str(dir3rawname) + json_file_suffix)
    with fpRaw.open("w") as f:
        json.dump(dummy_data_raw, f)

    raw_datafile = dir3raw / Path("raw_datafile.txt")
    with raw_datafile.open("w") as f:
        f.write("test data.txt")

    fpDeconv = dir3deconv / Path(str(dir3deconvname) + json_file_suffix)
    with fpDeconv.open("w") as f:
        json.dump(dummy_data_deconv, f)

    osfile = dir3deconv / Path(".DS_Store")
    with osfile.open("w") as f:
        f.write("")

    fpBadFilePth = dirraw / Path("dir3Chicken.json")
    with fpBadFilePth.open("w") as f:
        json.dump(dummy_data_bad, f)

    yield tempdir, dir1name, dir2name, dirraw, osfile

    shutil.rmtree(str(tempdir))


@fixture
def tmpdir_metadata_files():
    tempdir = None
    dir1 = None
    dir2 = None
    md_file_sfx = pc.METADATA_FILE_SUFFIX + "." + bc.JSON_FORMAT

    tempdir = Path("tempdir")
    if not tempdir.exists():
        tempdir.mkdir()

    dir1name = Path("dir1")
    dir1 = tempdir / dir1name
    if not dir1.exists():
        dir1.mkdir()

    dir1_md = {
        "name": "dir1",
        "description": "dummy description",
        "principal_investigator": "abcd123",
    }
    dir1md_fp = tempdir / Path(str(dir1name) + md_file_sfx)
    with dir1md_fp.open("w") as f:
        json.dump(dir1_md, f)

    dir2name = Path("dir2")
    dir2 = dir1 / dir2name
    if not dir2.exists():
        dir2.mkdir()

    dir2_md = {
        "title": "dir2",
        "description": "dummy description",
    }
    dir2md_fp = dir1 / Path(str(dir2name) + md_file_sfx)
    with dir2md_fp.open("w") as f:
        json.dump(dir2_md, f)

    dir3rawname = Path("dir3-Raw")
    dir3raw = dir2 / dir3rawname
    if not dir3raw.exists():
        dir3raw.mkdir()

    dir3raw_md = {
        "description": "dir3-Raw",
        "experiments": ["dir2"],
        "instrument": "some instrument",
    }
    dir3rawmd_fp = dir2 / Path(str(dir3rawname) + md_file_sfx)
    with dir3rawmd_fp.open("w") as f:
        json.dump(dir3raw_md, f)

    dir3deconvname = Path("dir3-Deconv")
    dir3deconv = dir2 / dir3deconvname
    if not dir3deconv.exists():
        dir3deconv.mkdir()

    dir3deconv_md = {
        "description": "dir3-Deconv",
        "experiments": ["dir2"],
        "instrument": "some instrument",
    }
    dir3deconvmd_fp = dir2 / Path(str(dir3deconvname) + md_file_sfx)
    with dir3deconvmd_fp.open("w") as f:
        json.dump(dir3deconv_md, f)

    dfilename = Path("datafile.txt")
    dfilepth = dir3raw / dfilename
    with dfilepth.open("w") as f:
        f.write("test")

    dfileraw_md = {
        "filename": "datafile.txt",
        "directory": str(dir3raw),
        "md5sum": "12345678910111213",
        "mimetype": ".txt",
        "size": 123456789,
        "dataset": str(dir3rawname),
    }
    dfilename_md_pth = dir3raw / Path(str(dfilename) + md_file_sfx)
    with dfilename_md_pth.open("w") as f:
        json.dump(dfileraw_md, f)

    bad_dfileraw_md = {
        "badfield": "badvalue",
    }
    bad_dfilename_md_pth = dir3raw / Path("bad_datafile" + md_file_sfx)
    with bad_dfilename_md_pth.open("w") as f:
        json.dump(bad_dfileraw_md, f)

    projs = [dir1md_fp]
    expts = [dir2md_fp]
    dsets = [dir3rawmd_fp, dir3deconvmd_fp]
    dfiles = [dfilename_md_pth]
    bad_dfiles = [bad_dfilename_md_pth]

    yield projs, expts, dsets, dfiles, bad_dfiles

    shutil.rmtree(str(tempdir))
