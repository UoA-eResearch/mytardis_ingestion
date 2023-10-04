# # pylint: disable=missing-function-docstring,redefined-outer-name,missing-module-docstring

# import json
# import os
# import shutil

# import responses
# from pytest import fixture

# from src.beneficiations.beneficiation import Beneficiation
# from src.beneficiations.parsers.json_parser import JsonParser
# from src.beneficiations import beneficiation_consts as bc
# from src.profiles import profile_consts as pc
# from src.extraction_output_manager.output_manager import OutputManager


# @fixture
# def spawn_bnfc():
#     json_parser = JsonParser()
#     bnfc = Beneficiation(json_parser)
#     yield bnfc
#     Beneficiation.clear()
