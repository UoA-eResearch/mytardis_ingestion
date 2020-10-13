from mech_test import MechTestFactory
from pathlib import Path

local_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_local.env')
global_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_global.env')
raw_data_dir = Path('/home/chris/Projects/MyTardisTestData/mechtests/raw data')
cyclic_data_dir = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/processed_data/full_curves')
tensile_data_dir = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/processed_data/tension_tests')
plot_dir = Path('/home/chris/Projects/MyTardisTestData/mechtests/TestPlots')
const_strain_tests = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/Constant-Strain-Test-List.dat')
project_json = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/project.json')
experiment_json = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/experiment.json')
top_data_dir = Path(
    '/home/chris/Projects/MyTardisTestData/mechtests/')
factory = MechTestFactory(local_config_file_path,
                          raw_data_dir,
                          const_strain_tests,
                          global_config_file_path)
