"""
UPDATED on Mon Sep 21 2021

@author: Marcus Maldonado
"""

import pandas as pd
from time import time
import os
import logging
from setup import snowflake, get_sql, project_path, config

start = time()

# SET CONFIG VARS
postal_retailer_decile = config['mvs']['postal_retailer_decile']
account_condition = config['mvs']['account_condition']
group_name1 = config['mvs']['group_name1']

# GET PROSCORE LOOKUP TABLE FROM HIVE
sql = get_sql(project_path / 'sql' / 'proscore_lookup.sql').format(group_name1)
proscore_lookup = pd.read_sql(sql, snowflake)
proscore_lookup.rename(columns={'mvs_name': 'mvs_name', 
                                'mvs_desc': 'value', 
                                'dimension': 'dimension', 
                                'group_name': 'group_name', 
                                'project_type': 'mvs_type', 
                                'definition': 'mvs_definition'}, inplace = True)

# check current columns in snowflake
df = pd.read_sql('describe table mvh_data_store', snowflake)[['name']].rename(columns = {'name': 'mvs_name'})
df['mvs_name'] = df['mvs_name'].str.lower()
proscore_lookup = proscore_lookup.merge(df)

#proscore_xref
proscore_lookup['value'] = proscore_lookup['value'].str.lower()

## create proscore_string
proscore_string = 'AVG({mvs_name}) as "{mvs_name}", '
proscore_string = [proscore_string.format(mvs_name = row['mvs_name'].strip()) for _, row in proscore_lookup.iterrows()]
proscore_string = ''.join(proscore_string)[:-2] # remove last comma and space

# create a dictionary, which will be used to rename proscore columns later
proscore_dictionary = {row['mvs_name'].strip(): row['value'].strip() for _, row in proscore_lookup.iterrows()}

###############################################################################

# create subfolder called "Demos" if it does not already exist
if not os.path.exists('./Demos'):
    os.makedirs('./Demos')

# create subfolder called "MVS_Files" if it does not already exist to hold MVS
# store attributes and proscores
if not os.path.exists('./MVS_Files'):
    os.makedirs('./MVS_Files')

# pull dx_id and store attributes
logging.info('Pulling Store Attributes...')
sql = get_sql(project_path / 'sql' / 'dx_stores.sql').format(account_condition)
      
tmp_stores = pd.read_sql(sql, con=snowflake)
logging.info('%d stores found for retailer.' % len(tmp_stores['dx_id'].unique()))

# Write the dataframe contents to a csv file
tmp_stores.to_csv('./MVS_Files/mvs_store_attr.csv', index = False)

###############################################################################
################################## DATA_PULL ##################################
#################### store lod ######################
###############################################################################
logging.info('Pulling store LOD assignments...')
sql = get_sql(project_path / 'sql' / 'store_lod.sql').format(account_condition = account_condition, postal_retailer_decile = postal_retailer_decile)
store_lod = pd.read_sql(sql, snowflake)

assert len(tmp_stores.index) == len(store_lod.index), 'Store LOD assignments resulted in the wrong number of stores!'
dx_ids = tuple(store_lod[store_lod['lod'] == 'dx_id']['dx_id'].tolist())
zip5s = tuple(store_lod[store_lod['lod'] == 'zip5']['zip5'].unique().tolist())
zip3s = tuple(store_lod[store_lod['lod'] == 'zip3']['zip3'].unique().tolist()) 
#if retailer is small, this will create an error in SQL: 'Zip3 IN ['641, ]' - need to adjust for smaller retailers with one zip3
#zip3s = '641', make sure to change IN to '=' for zip3 in proscore and demos by zip

###############################################################################
################################## DATA_PULL ##################################
################################## demos ####################################
###############################################################################
logging.info('Pulling demos by dx_id...')
sql = get_sql(project_path / 'sql' / 'demos_by_dx_id.sql').format(dx_ids = dx_ids)
df = pd.read_sql(sql, snowflake)
demos_dx = store_lod.merge(df, on = ['dx_id', 'lod'])

logging.info('Pulling demos by zip...')
sql = get_sql(project_path / 'sql' / 'demos_by_zip.sql').format(postal_retailer_decile = postal_retailer_decile, zip5s = zip5s, zip3s = zip3s)
df = pd.read_sql(sql, snowflake)
demos_zip5 = store_lod.merge(df[df['lod'] == 'zip5'], left_on = ['zip5', 'lod'], right_on = ['zip', 'lod'])
demos_zip3 = store_lod.merge(df[df['lod'] == 'zip3'], left_on = ['zip3', 'lod'], right_on = ['zip', 'lod'])

demos = pd.concat([demos_dx, demos_zip5, demos_zip3])
demos = demos.drop(columns = ['zip'])

logging.info('Exporting demos...')
#assert len(tmp_stores.index) == len(demos['dx_id'].unique()), 'Demo data pull resulted in the wrong number of stores!'
if len(tmp_stores.index) != len(demos['dx_id'].unique()):
    logging.warning('Missing HH coverage for the following DX IDs:\n%s' % store_lod[~store_lod['dx_id'].isin(demos['dx_id'].unique())])
    tmp_stores = tmp_stores[tmp_stores['dx_id'].isin(demos['dx_id'].unique())]
    store_lod = store_lod[store_lod['dx_id'].isin(tmp_stores['dx_id'].unique())]
    assert len(tmp_stores.index) == len(store_lod.index), 'Store LOD and attr mismatch!'
    tmp_stores.to_csv('./MVS_Files/mvs_store_attr.csv', index = False)
    logging.warning('Stores overwritten. Count updated to %d.' % len(tmp_stores.index))

demos.to_csv(str(project_path / 'Demos' / 'demos.csv'), index = False)


###############################################################################
################################## DATA_PULL ##################################
################################# Proscores ###################################
###############################################################################

logging.info('Pulling proscores by dx_id...')
sql = get_sql(project_path / 'sql' / 'proscores_by_dx_id.sql').format(proscore_string = proscore_string, dx_ids = dx_ids)
df = pd.read_sql(sql, snowflake)  
proscores_dx = store_lod.merge(df, on = ['dx_id', 'lod'])

logging.info('Pulling proscores by zip...')
sql = get_sql(project_path / 'sql' / 'proscores_by_zip.sql').format(proscore_string = proscore_string, postal_retailer_decile = postal_retailer_decile, zip5s = zip5s, zip3s = zip3s)
df = pd.read_sql(sql, snowflake)
proscores_zip5 = store_lod.merge(df[df['lod'] == 'zip5'], left_on = ['zip5', 'lod'], right_on = ['zip', 'lod'])
proscores_zip3 = store_lod.merge(df[df['lod'] == 'zip3'], left_on = ['zip3', 'lod'], right_on = ['zip', 'lod'])

proscores = pd.concat([proscores_dx, proscores_zip5, proscores_zip3])
proscores = proscores.drop(columns = ['zip', 'zip5', 'zip3', 'lod'])

# rename columns to make column names meaningful to business team
proscores = proscores.rename(columns = proscore_dictionary)

logging.info('Exporting proscores...')
assert len(tmp_stores.index) == len(proscores['dx_id'].unique()), 'Proscores data pull resulted in the wrong number of stores!'
proscores.to_csv(str(project_path / 'MVS_files' / 'avg_proscores_by_store.csv'), index = False)

logging.info('%s completed in %0.1f minutes' % (__name__, (time() - start) / 60))