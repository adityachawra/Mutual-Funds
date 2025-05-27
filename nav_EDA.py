import pandas as pd

import json
from json_to_dict import get_nav_data



nav = get_nav_data()


nav_dfexp = pd.DataFrame(nav)

nav_dfexp.set_index('date', inplace=True)
nav_dfexp = nav_dfexp.apply(pd.to_numeric, errors='coerce')
for columns in nav_dfexp.columns:
    nav_dfexp[columns] = nav_dfexp[columns].interpolate(method='linear', axis=0)
        

nav_dfexp.reset_index(inplace=True)

nav_df = nav_dfexp
