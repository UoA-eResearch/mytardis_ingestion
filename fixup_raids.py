import pandas as pd
from Core.helpers import readJSON, writeJSON
from pathlib import Path

existing_json_path = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment_exist.json')
experiment_raid_json = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment_raid.json')
working = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment_all_for_comp.xlsx')
fixup = Path(
    '/home/chris/Projects/PrintGroupGenomics/experiment_fixup.xlsx')

comps = {}
trb = []
trb_raid = []
trb_real_expt = []
experiments = readJSON(experiment_raid_json)
experiments_reversed = {value: key for key, value in experiments.items()}
all_exp = []
all_raid = []
for data in experiments.keys():
    all_exp.append(data)
    all_raid.append(experiments[data])
construct = {}
all_data = readJSON(existing_json_path)
reserved = []
reserved_raid = []
count = 0
comps = {}
for data in all_data['objects']:
    if data['title'].startswith('Constant'):
        continue
    if data['title'] == 'AM-1':
        count += 1
        continue
    if data['title'] in reserved:
        trb.append(data['title'])
        trb_raid.append(data['raid'])
        trb_real_expt.append(data['project']['name'])
    else:
        construct[data['title']] = data['raid']
        reserved.append(data['title'])
        reserved_raid.append(data['raid'])
        comps[data['title']] = data['raid']

print(count)
print(len(reserved_raid)-len(set(reserved_raid)))

for i in reserved_raid:
    all_raid.remove(i)

for i in reserved:
    print(i)
    all_exp.remove(i)

print(len(all_exp), len(all_raid))

for i in range(len(all_exp)):
    if all_exp[i] in reserved:
        continue
    else:
        construct[all_exp[i]] = all_raid.pop()

df = pd.DataFrame(construct.items())
df.columns = ['Name', 'RAiD']
df['Check'] = df['Name'].map(comps)
df.to_excel(working)

const_rev = {value: key for key, value in construct.items()}

df2 = pd.DataFrame()
df2['Names'] = trb
df2['Raid'] = trb_raid
df2['Projects'] = trb_real_expt
df2['To Fix'] = df2['Raid'].map(const_rev)
df2.to_excel(fixup)

writeJSON(construct,
          experiment_raid_json)
'''print(count)
print(count2)
print(count3)
print(len(comps))
print(count-(count2+count3))


outs = {}

for key in experiments.keys():
    outs[key] = experiments[key]

comps_rev = {value: key for key, value in comps.items()}

print(len(comps_rev))

for i in range(len(trb)):
    print(trb[i], trb_raid[i], trb_real_expt[i])

print(len(trb))

keys = list(comps.keys())
print(len(keys))
count = 0
count2 = 0
swaps = []
for i in range(len(keys)):
    comp = keys[i]
    if comp == 'AM-1':
        count += 1
        continue
    else:
        real_raid = comps[comp]
        current_raid = experiments[comp]
        current_exp = experiments_reversed[real_raid]
        swaps.append((comp, real_raid, current_raid, current_exp))

print(len(swaps))
print(swaps)

print(count)
df1 = pd.DataFrame(comps.items())
df1.columns = ['Name', 'Real Raid']
print(len(set(df1['Real Raid'])))
print(len(set(df1['Name'])))
df2 = pd.DataFrame(experiments.items())
df2.columns = ['Names', 'Current Raids']
df = df2.merge(df1, how='left', left_on='Names', right_on='Name')
df['Current Expt with Raid'] = df['Real Raid'].map(experiments_reversed)
df['Fixed Raids'] = df['Names'].map(outs)
df.to_excel(working)
print(f'AM-1: {comps["AM-1"]} - {experiments_reversed[comps["AM-1"]]}')
extras = list(set(df['Current Raids'])-set(df['Fixed Raids']))
for i in range(len(keys)):
    comp = keys[i]
    if comp == 'AM-1':
        continue
    elif comp[-4:] == '_BAD':
        continue
    else:
        real_raid = comps[comp]
        current_raid = None
        if comp in experiments.keys():
            current_raid = experiments[comp]
        current_exp = experiments_reversed[real_raid]
        outs[comp] = real_raid
        comps[current_exp] = current_raid'''
