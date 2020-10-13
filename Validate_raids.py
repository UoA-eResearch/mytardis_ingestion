import json
from pathlib import Path
from Core.helpers import RAiDFactory
import pandas as pd

Cyclic_raids = ['10378.1/1634823',
                '10378.1/1634862',
                '10378.1/1635085',
                '10378.1/1634819',
                '10378.1/1634860',
                '10378.1/1634837',
                '10378.1/1634872',
                '10378.1/1635063',
                '10378.1/1634859',
                '10378.1/1634856',
                '10378.1/1634864',
                '10378.1/1634890',
                '10378.1/1634847',
                '10378.1/1634844',
                '10378.1/1634845',
                ]

global_config_file_path = Path(
    '/home/chris/Projects/MyTardisTestData/mech_global.env')
cyclic_raids_file = Path(
    '/home/chris/Projects/PrintGroupGenomics/project_raids.xlsx')

raid_factory = RAiDFactory(global_config_file_path)

cyclic = pd.DataFrame(columns=['Name', 'RAiD', 'URL'])

for raid in Cyclic_raids:
    response = raid_factory.get_raid(raid)
    resp_dict = json.loads(response.text)
    url = resp_dict['contentPath']
    meta = resp_dict['meta']
    name = meta['name']
    cyclic = cyclic.append(
        {'Name': name, 'RAiD': raid, 'URL': url}, ignore_index=True)

print(cyclic)
cyclic.to_excel(cyclic_raids_file)
