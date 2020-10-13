import json
from pathlib import Path
from Core.helpers import RAiDFactory

project_json = Path('/home/chris/Projects/PrintGroupGenomics/project.json')
experiment_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment.json')
project_raid_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/project_raids.json')
experiment_raid_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment_raids.json')
old_raid_json = Path('/home/chris/Projects/Old_Raids.json')
global_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_global.env')


def readJSON(json_file):
    try:
        with open(json_file, 'r') as in_file:
            json_dict = json.load(in_file)
    except Exception as err:
        raise
    return json_dict


def writeJSON(json_dict, json_file):
    try:
        with open(json_file, 'w') as out_file:
            json.dump(json_dict, out_file, indent=4)
        return True
    except Exception as err:
        raise


raid_factory = RAiDFactory(global_config_file_path)
projects = readJSON(project_json)
experiments = readJSON(experiment_json)
project_raids = {}
for proj in projects['projects']:
    project_raids[proj["name"]] = proj['raid']

writeJSON(project_raids, project_raid_json)
'''old_raids = readJSON(old_raid_json)
expt_raids = []
for raid in old_raids['raids']:
    expt_raids.append(raid['handle'])

for i in range(len(expt_raids)):
    expt = experiments['experiments'][i]
    expt['raid'] = expt_raids[i]

'''

ret_list = []
response = raid_factory.get_all_raids()
resp_dict = json.loads(response.text)
raids = resp_dict['items']
fix_up = {}
for raid in raids:
    fix_up[raid['raidName']] = raid['handle']

expt_raids = {}
'''for experiment in experiments['experiments']:
    if 'raid' in experiment.keys():
        raid = experiment['raid']
        print('old raid')
    else:
        name = 'PGG-' + str(experiment['title'])
        description = 'PGG sample'
        url = 'https://www.google.co.nz'
        if name in fix_up.keys():
            raid = fix_up[name]
            experiment['raid'] = raid
            print('found Raid')
        else:
            response = raid_factory.mint_raid(name,
                                              description,
                                              url)
            resp_dict = json.loads(response.text)
            raid = resp_dict["handle"]
            experiment['raid'] = raid
            print('minted Raid')
    expt_raids[experiment['title']] = raid'''

'''writeJSON(experiments, experiment_json)
writeJSON(expt_raids, experiment_raid_json)'''

print(experiments)
