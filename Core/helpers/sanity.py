from .exceptions import SanityCheckError


def sanity_check(input_dict,
                 required_keys):
    if not (required_keys - input_dict.keys()):
        return True
    else:
        missing_keys = list(required_keys - input_dict.keys())
        raise SanityCheckError(input_dict,
                               missing_keys)
