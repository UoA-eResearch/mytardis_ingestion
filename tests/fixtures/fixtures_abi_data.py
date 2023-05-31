# pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring

import json
import os
import shutil

import responses
from pytest import fixture

from src.beneficiations import beneficiation_consts as bc
from src.profiles import profile_consts as pc
from src.profiles.output_manager import OutputManager


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

    tempdir = "tempdir"
    if not os.path.exists(tempdir):
        os.mkdir(tempdir)

    dir1name = "dir1"
    dir1 = os.path.join(tempdir, dir1name)
    if not os.path.exists(dir1):
        os.mkdir(dir1)

    dir2name = "dir2"
    dir2 = os.path.join(dir1, dir2name)
    if not os.path.exists(dir2):
        os.mkdir(dir2)

    dir3rawname = "dir3-Raw"
    dir3raw = os.path.join(dir2, dir3rawname)
    if not os.path.exists(dir3raw):
        os.mkdir(dir3raw)

    dir3deconvname = "dir3-Deconv"
    dir3deconv = os.path.join(dir2, dir3deconvname)
    if not os.path.exists(dir3deconv):
        os.mkdir(dir3deconv)

    dirrawname = "dir-Raw"
    dirraw = os.path.join(dir2, dirrawname)
    if not os.path.exists(dirraw):
        os.mkdir(dirraw)

    dir3name = "dir3"

    dummy_data_raw = {
        "Basename": {"Project": dir1name, "Sample": dir2name, "Sequence": dir3name},
        "Description": "Dummy Description",
        "SequenceID": 123456789,
        "Camera Settings": {"Fan": False, "Pixel Correction": False, "Cooling": False},
    }

    dummy_data_deconv = {
        "Basename": {"Project": dir1name, "Sample": dir2name, "Sequence": dir3name},
        "Description": "Dummy Description",
        "SequenceID": 123456789,
        "Camera Settings": {"Fan": False, "Pixel Correction": False, "Cooling": False},
    }

    dummy_data_bad = {
        "Basename": {"Project": dir1name, "Sample": dir2name, "Sequence": dirrawname},
        "Description": "Dummy Description",
        "SequenceID": 123456789,
        "Camera Settings": {"Fan": False, "Pixel Correction": False, "Cooling": False},
    }

    json_file_suffix = ".json"
    fpRaw = os.path.join(dir3raw, dir3rawname + json_file_suffix)
    with open(fpRaw, "w") as f:
        json.dump(dummy_data_raw, f)

    raw_datafile = os.path.join(dir3raw, "raw_datafile.txt")
    with open(raw_datafile, "w") as f:
        f.write("test data.txt")

    fpDeconv = os.path.join(dir3deconv, dir3deconvname + json_file_suffix)
    with open(fpDeconv, "w") as f:
        json.dump(dummy_data_deconv, f)

    osfile = os.path.join(dir3deconv, ".DS_Store")
    with open(osfile, "w") as f:
        f.write("")

    fpBadFilePth = os.path.join(dirraw, "dir3Chicken.json")
    with open(fpBadFilePth, "w") as f:
        json.dump(dummy_data_bad, f)

    yield tempdir, dir1name, dir2name, dirraw, osfile

    shutil.rmtree(str(tempdir))


@fixture
def tmpdir_metadata_files():
    tempdir = None
    dir1 = None
    dir2 = None
    md_file_sfx = pc.METADATA_FILE_SUFFIX + "." + bc.JSON_FORMAT

    tempdir = "tempdir"
    if not os.path.exists(tempdir):
        os.mkdir(tempdir)

    dir1name = "dir1"
    dir1 = os.path.join(tempdir, dir1name)
    if not os.path.exists(dir1):
        os.mkdir(dir1)

    dir1_md = {
        "name": "dir1",
        "description": "dummy description",
        "principal_investigator": "abcd123",
    }
    dir1md_fp = os.path.join(tempdir, dir1name + md_file_sfx)
    with open(dir1md_fp, "w") as f:
        json.dump(dir1_md, f)

    dir2name = "dir2"
    dir2 = os.path.join(dir1, dir2name)
    if not os.path.exists(dir2):
        os.mkdir(dir2)

    dir2_md = {
        "title": "dir2",
        "description": "dummy description",
    }
    dir2md_fp = os.path.join(dir1, dir2name + md_file_sfx)
    with open(dir2md_fp, "w") as f:
        json.dump(dir2_md, f)

    dir3rawname = "dir3-Raw"
    dir3raw = os.path.join(dir2, dir3rawname)
    if not os.path.exists(dir3raw):
        os.mkdir(dir3raw)

    dir3raw_md = {
        "description": "dir3-Raw",
        "experiments": ["dir2"],
        "instrument": "some instrument",
    }
    dir3rawmd_fp = os.path.join(dir2, dir3rawname + md_file_sfx)
    with open(dir3rawmd_fp, "w") as f:
        json.dump(dir3raw_md, f)

    dir3deconvname = "dir3-Deconv"
    dir3deconv = os.path.join(dir2, dir3deconvname)
    if not os.path.exists(dir3deconv):
        os.mkdir(dir3deconv)

    dir3deconv_md = {
        "description": "dir3-Deconv",
        "experiments": ["dir2"],
        "instrument": "some instrument",
    }
    dir3deconvmd_fp = os.path.join(dir2, dir3deconvname + md_file_sfx)
    with open(dir3deconvmd_fp, "w") as f:
        json.dump(dir3deconv_md, f)

    dfilename = "datafile.txt"
    dfilepth = os.path.join(dir3raw, dfilename)
    with open(dfilepth, "w") as f:
        f.write("test")

    dfileraw_md = {
        "filename": "datafile.txt",
        "directory": dir3raw,
        "md5sum": "12345678910111213",
        "mimetype": ".txt",
        "size": 123456789,
        "dataset": dir3rawname,
    }
    dfilename_md_pth = os.path.join(dir3raw, dfilename + md_file_sfx)
    with open(dfilename_md_pth, "w") as f:
        json.dump(dfileraw_md, f)

    bad_dfileraw_md = {
        "badfield": "badvalue",
    }
    bad_dfilename_md_pth = os.path.join(dir3raw, "bad_datafile" + md_file_sfx)
    with open(bad_dfilename_md_pth, "w") as f:
        json.dump(bad_dfileraw_md, f)

    projs = [dir1md_fp]
    expts = [dir2md_fp]
    dsets = [dir3rawmd_fp, dir3deconvmd_fp]
    dfiles = [dfilename_md_pth]
    bad_dfiles = [bad_dfilename_md_pth]

    yield projs, expts, dsets, dfiles, bad_dfiles

    shutil.rmtree(str(tempdir))
