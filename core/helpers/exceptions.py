'''MyTardis ingestion specific exceptions'''


class Error(Exception):
    """Base class for exceptions in this module."""
    pass


class SanityCheckError(Error):
    '''Custom exception for when a sanity check has been failed.
    Returns the object type, input dictionary and missing keys'''

    def __init__(self,
                 input_dict,
                 missing_keys):
        self.input_dict = input_dict
        self.missing_keys = missing_keys


class UnableToFindUniqueError(Error):
    '''Custom exception for when a search returns a non-unique
    database entry. Pass up the response for further analysis
    should it be needed.'''

    def __init__(self,
                 action,
                 query_params,
                 response_dict):
        self.action = action
        self.query_params = query_params
        self.resonse_dict = response_dict


class HierarchyError(Error):
    '''Custom esception for when the parent object of an object
    in the MyTardis database cannot be found.'''

    def __init__(self,
                 parent_obj,
                 child_obj):
        self.parent_obj = parent_obj
        self.child_obj = child_obj
