import pandas as pd

import json
from json_to_dict import get_nav_data, get_specs_data



nav = get_nav_data()


nav_dfexp = pd.DataFrame(nav)

nav_dfexp.set_index('date', inplace=True)
nav_dfexp = nav_dfexp.apply(pd.to_numeric, errors='coerce')
for columns in nav_dfexp.columns:
    nav_dfexp[columns] = nav_dfexp[columns].interpolate(method='linear', axis=0)
        

nav_dfexp.reset_index(inplace=True)

specs = get_specs_data()
specs_df = pd.DataFrame(specs) 
specs_df = specs_df.transpose()
specs_df['main_type'] = specs_df['category'].str.extract(r'^(.*?)\s*\(')
specs_df['inner'] = specs_df['category'].str.extract(r'\(\s*(.*?)\s*\)')
specs_df[['sub_type', 'specific_type']] = specs_df['inner'].str.split(' - ', expand=True)

# Drop the intermediate column
specs_df.drop(columns='inner', inplace=True)
specs_df.reset_index(names = "Scheme Code",inplace=True)


nav_df = nav_dfexp.copy(deep = True)

