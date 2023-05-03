import hashlib
import mimetypes

from src.profiles import profile_consts as pc


def create_dataclass_entry_dict(
                                ) -> dict:
    return dict.fromkeys(pc.DATACLASS_ENTRY_DICT_KEYS)


def create_output_dict(
                       ) -> dict:
    return dict.fromkeys(pc.OUTPUT_DICT_KEYS)


def create_output_subdict(
                          ) -> dict:
    return dict.fromkeys(pc.OUTPUT_SUBDICT_KEYS)


def format_field_for_key_seq(
                            key_seq: list,
                            ) -> str:
    frmtd_key = key_seq[0]
    for i in range(len(key_seq)-1):
        frmtd_key += pc.KEY_LVL_SEP + key_seq[i+1]
    return frmtd_key


def format_key_for_indexed_field(
                                key: str,
                                idx: int,
                                ) -> str:
    frmtd_key = key
    frmtd_key += pc.KEY_IDX_OP + str(idx) + pc.KEY_IDX_CL
    return frmtd_key


def calculate_md5sum(
                    fp: str,
                    ) -> str:
    with open(fp, mode='rb') as f:
        d = hashlib.md5()
        for buf in iter(lambda: f.read(128 * 1024), b''):
            d.update(buf)
    return d.hexdigest()


def determine_mimetype(
                       fn: str,
                      ) -> str:
    mimetype, encoding = mimetypes.guess_type(fn)
    return mimetype