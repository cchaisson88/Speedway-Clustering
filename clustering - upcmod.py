"""
@author: Marcus Maldonado

UPC modification from the original, using the Pareto 80/20 rule.
"""

######################## Notes and general requirements########################
# HHs is shorthand for "households"

# All sales files MUST be in a subfolder called "Sales"

# All MVS demos files MUST be in a subfolder called "Demos"

# The string "<< MANUAL_CUSTOMIZE >>" has been inserted anywhere changes are
# needed before running the code for a new project

#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
#update the file name to reference data in Tableau
# =============================================================================

###################### REQUIRED FIELDS BY REQUIRED FILE #######################
# store attribute file

# product attribute file

# sales file(s)

# HH demographic file
# 1) demo column names string MUST be the same as the respective file name

# proscore file

# Pause code to confirm results?
# It is recommended that this be set to True initially to validate datatypes
# and field names.  It essentially runs the code in verbose mode, sending more
# information to the console.
# << MANUAL_CUSTOMIZE >>
Confirm = True

################################## QuickRefs ##################################
# Supported numpy data types: https://docs.scipy.org/doc/numpy-1.15.0/user/basics.types.html
# all public pandas objects, functions and methods: https://pandas.pydata.org/pandas-docs/version/0.23.4/api.html#id5
# pandas cookbok: https://pandas.pydata.org/pandas-docs/version/0.23.4/cookbook.html#cookbook
# read_csv doc: https://pandas.pydata.org/pandas-docs/stable/generated/pandas.read_csv.html
# built-in functions: https://docs.python.org/3/library/functions.html

# DOUG look at load sections to see if there is anything to move to either the clean&validate or process sections

###############################################################################
#################################### SETUP ####################################
###############################################################################
import glob # https://docs.python.org/3/library/glob.html
import os # https://docs.python.org/3/library/os.html
import time # https://docs.python.org/3/library/time.html
import pandas as pd # https://pandas.pydata.org/
import numpy as np # http://www.numpy.org/
from zipfile import ZipFile
import matplotlib.pyplot as plt # Python plotting library
from sklearn.cluster import KMeans # KMeans clustering
from sklearn.decomposition import PCA # Principal Component Analysis module
from sklearn.preprocessing import StandardScaler
import subprocess
import logging
from setup import project_path

start_time = time.time()

# Specify working directory containing input files (this should be the same 
# path specified when running the SQL code to pull MVS data)
# << MANUAL_CUSTOMIZE >> # 
os.chdir(str('C:\\Users\\80207640\\OneDrive - Pepsico\\DESKTOP\\DX Folder\\Clustering Projects 2023\\Albertsons\\clustering-adhoc'))
# save the working directory path for later use
dirpath = os.getcwd()

# import custom modules
from find_encoding import find_encoding

# create subfolder called "Output" if it does not already exist
if not os.path.exists('./Output'):
    os.makedirs('./Output')

# << MANUAL_CUSTOMIZE >> # 
# Is there a geographic attribute that will be used to index the stores before
# clustering?  If yes, set this to True, else set to False
# DOUG this can be removed if we are no longer doing indices
have_regions = False

# << MANUAL_CUSTOMIZE >> # 
# Are there any other sales files provided other than the main sales files?
# These would be placed in the folder called 'All Other Product Aggregates'
# and would be sales for products that are not the primary focus of the project,
# but still need to be included in the analysis.  This is rare.
# False = no other sales files{proscore_string}
other_product_sales = False

###############################################################################
######################## Load files and rename columns ########################
###############################################################################

##################### BEGIN load mvs store attribute file #####################

print('Loading mvs store attributes...\r\n')

mvs_store_attr = pd.read_csv('./MVS_Files/mvs_store_attr.csv',
                            encoding= find_encoding('./MVS_Files/mvs_store_attr.csv'))

# DOUG check here for duplicate btg_store_nbr, list the dups, and quit

######################## END mvs load store attributes ########################


################## BEGIN load retailer store attribute file ###################
################################## STORES ##################################
    
# DOUG - code needs to handle cases when store unique ID is more than one field
# DOUG 1.0 need to add steps before the loads so the user can see the field names in the files and know what to enter for the data types
print('Loading store attributes...\r\n')

# << MANUAL_CUSTOMIZE >> #
# We want to designate the file name and data types of any fields that may/will 
#     be misinterpreted by read_csv
store_attr = pd.read_csv('./Retailer_Files/Store Attributes _Final 10_25.csv',
                        encoding= find_encoding('./Retailer_Files/Store Attributes _Final 10_25.csv'),
                         dtype = 
                         {
                                    'Geography': str,
                                    'Retailer Store Number': str,
                                    'Banner Name': str,
                                    'Retailer Name': str,
                                    'TDLinx': str,
                                    'Division': str,
                                    'Street Address': str,
                                    'City': str,
                                    'State Code': str,
                                    'ZIP Code': str,
                                    'County': str,
                                    'Latitude': str,
                                    'Longitude': str  
                                 },)


# convert all column headers to lower case
store_attr.columns = map(str.lower, store_attr.columns)

# trim column names
store_attr.columns = store_attr.columns.str.strip()

# add column to indicate which stores should be clustered, defaulting all to True
# DOUG with Target testing the field and values are already set so uncomment after testing
store_attr['include_in_clusters'] = True

# DOUG need to add code to change 'include_in_clusters' to False based on criteria

# DOUG change all user input lines to accept Enter to continue and Esc to break
# and include a try except module to evaluate user input

# Count stores we are clustering (True) and not clustering (False)
if Confirm:
    print('VERIFY the number of stores set to TRUE is the number of stores to cluster:')
    print(store_attr.groupby('include_in_clusters').size())
    if Confirm:input("Press Enter to continue...\r\n")

# rename store_attr fields
# postal code field must be renamed to 'postal_code'
# regionality field (if it exists) must be renamed to 'region'
# column names from source MUST BE LOWERCASE REGARDLESS OF HOW THEY ARE FORMATTED IN FILE
# << MANUAL_CUSTOMIZE >> #

store_attr.rename(columns = {
                                    'geography': 'store_nme',
                                    'retailer store number': 'parent_name',
                                    'banner name': 'banner_name',                                    
                                    'retailer name': 'retailer',
                                    'tdlinx': 'tdlinx',
                                    'division': 'division',
                                    'street address': 'address',
                                    'city': 'city',
                                    'state code': 'state',
                                    'zip code': 'postal_code',
                                    'county': 'county',
                                    'latitude': 'latitude',
                                    'longitude': 'longitude',
         
        #'outlet Name': 'outlet_type',
         'dxid': 'dx_id'       
        }, inplace = True)

# << MANUAL_CUSTOMIZE >> #
# DO NOT INCLUDE if you have already included DX_IDs
# merge store_attr with mvs_store_attr so we can add the dx_id
# specify the field name in store_attr that is the stores' primary key in
# the left_on parameter
#store_attr = store_attr.merge(mvs_store_attr[['btg_store_nbr','dx_id']], how = 'left', left_on = 'store_nbr', right_on = 'btg_store_nbr')
#store_attr = store_attr.merge(mvs_store_attr[['tdlinx_id','dx_id']], how = 'left', left_on = 'tdlinx', right_on = 'tdlinx_id')

# Check for stores that we are clustering and that are missing dx_ids (nan) 
# and check for duplicate dx_ids.
#if (store_attr.store_nbr[(store_attr.dx_id.isna()) & store_attr.include_in_clusters == True].count() > 0 or
#store_attr.dx_id[store_attr.dx_id.duplicated()].nunique() > 0):
if (store_attr.tdlinx[(store_attr.dx_id.isna()) & store_attr.include_in_clusters == True].count() > 0 or
store_attr.dx_id[store_attr.dx_id.duplicated()].nunique() > 0):
    print('\r\n ERROR : ' + str(store_attr.tdlinx[store_attr['dx_id'].isna() & store_attr.include_in_clusters == True].count()) + 
          ' stores are missing dx_ids and there are ' + 
          str(store_attr.dx_id[store_attr['dx_id'].duplicated()].nunique()) + 
          ' duplicate dx_ids\r\n')
    
    #if store_attr.store_nbr[(store_attr.dx_id.isna()) & store_attr.include_in_clusters == True].count() > 0:
    #    print('Store numbers missing dx_ids...')
    #    print(store_attr.store_nbr[(store_attr.dx_id.isna())].to_string(index=False))
    if store_attr.tdlinx[(store_attr.dx_id.isna()) & store_attr.include_in_clusters == True].count() > 0:
        print('Store numbers missing dx_ids...')
        print(store_attr.tdlinx[(store_attr.dx_id.isna())].to_string(index=False))
        
    if store_attr.dx_id[store_attr.dx_id.duplicated()].nunique() > 0:
        print('\r\nDupliated dx_ids...')
        print(store_attr.dx_id[store_attr.dx_id.duplicated() & ~store_attr.dx_id.isna()].to_string(index=False))
    
#    input("Press Enter to QUIT...\r\n")
#    quit() # DOUG how to quit without clearing the console of the relevant info??
    if Confirm:input("Press Enter to continue...\r\n")

# Check number of stores flagged for clustering (assumption being that this
# flag is set in the CSV file before code execution)
# adjusted to dx_id instead of tdlinx as we are not using tdlinx to obtain dxids
print(str(store_attr.dx_id[store_attr.include_in_clusters == True].count())
        + ' stores are flagged for clustering\r\n') # count stores in pca_df
if Confirm:input("Press Enter to continue...\r\n")

# if no regions exist, add a dummy field to store_attr so the indexing code works
# DOUG this can be removed if we are no longer doing indices
if not have_regions:
    store_attr['region'] = 'total'

# Create list of fields from store_attr that will be used to aggregate store
# measures.  dx_id must always be included from MVS, but include the
# regionality field if the retailer provided one, else "region" should be included
# DOUG s_attr_to_include references can be removed if we are no longer
# indexing against regions before PCA
# << MANUAL_CUSTOMIZE >> #
s_attr_to_include = [
        'dx_id',
        'region'
        ]

# Verify column names and data types
if Confirm:
    print('VERIFY STORE ATTRIBUTE COLUMN NAMES AND DATA TYPES:')
    print(store_attr.dtypes)
    input("Press Enter to continue...\r\n")

########################## END load store attributes ##########################


######################## BEGIN load product attributes ########################
print('Loading product attributes...\r\n')

# We want to designate the retailer product attribute file and data types of 
# any fields that may/will be misinterpreted by read_csv 
# << MANUAL_CUSTOMIZE >> #
prod_attr = pd.read_csv('./Retailer_Files/Product Attributes.csv', 
                        encoding= find_encoding('./Retailer_Files/Product Attributes.csv'),
                        dtype =
                        {
                        'Geography': str,
                        'Product': str,
                        'Dollar Sales': float,
                        'Unit Sales': float,
                        'UPC 13': str,
                        'UPC 10': str,
                        'TSA - Brand': str,
                        'TSA - Manufacturer': str,
                        'TSA - Parent': str,
                        'TSA - Type': str,
                        'Custom MS Flavor Bucket': str,
                        'Custom MS Frito Salty Snacks Size': str,
                        'Custom MS Line Extension': str,	
                        'Custom MS Manufacturer': str,	
                        'Custom MS Parent Company': str,	
                        'Custom MS Price Tier': str,	
                        'Custom MS Serving Size': str,	
                        'Custom MS Sub Category': str,	
                        'Custom MS Take Home or Single Serve': str,	
                        'Custom MS Trademark': str
                        })

# trim column names
prod_attr.columns = prod_attr.columns.str.strip()

# << MANUAL_CUSTOMIZE >> #
# rename any fields to be used for PCA and/or profiling (must include UPC)

prod_attr.rename(columns = 
             {
                   'Geography': 'retailer_name',
                        'Product': 'item_desc',
                        'Dollar Sales': 'prdt_dollar_sales',
                        'Unit Sales': 'prdt_unit_sales',
                        'UPC 13': 'upc_13',
                        'UPC 10': 'upc_10',
                        'TSA - Brand': 'tsa_brand',
                        'TSA - Manufacturer': 'tsa_manufacturer',
                        'TSA - Parent': 'tsa_parent',
                        'TSA - Type': 'tsa_type',
                        'Custom MS Flavor Bucket': 'flavor_bucket',
                        'Custom MS Frito Salty Snacks Size': 'size',
                        'Custom MS Line Extension': 'line_extension',	
                        'Custom MS Manufacturer': 'manufacturer',	
                        'Custom MS Parent Company': 'parent_company',	
                        'Custom MS Price Tier': 'price_tier',	
                        'Custom MS Serving Size': 'serving_size',	
                        'Custom MS Sub Category': 'sub_category',	
                        'Custom MS Take Home or Single Serve': 'take_home',	
                        'Custom MS Trademark': 'trademark'                
              },
              inplace = True)

# Specify what the product unique identifier should be between sales and product files
product_unique_identifier = 'upc_10'

############## GET TOP UPC'S ################ MM 2022
logging.info('Filtering tail upcs...')
# << MANUAL_CUSTOMIZE >> #
# rename measures where necessary (ex: for save a lot, dollar sales (non-projected) renamed to dollar sales)
top_upcs = prod_attr[[product_unique_identifier, 'prdt_dollar_sales']].sort_values('prdt_dollar_sales', ascending = False)
top_upcs['prdt_dollar_sales'] = top_upcs['prdt_dollar_sales'].fillna(0)

weight = top_upcs['prdt_dollar_sales'].sum() * 0.9 # PARETO RULE

for i in range(len(top_upcs.index)):
    if top_upcs.iloc[0:i]['prdt_dollar_sales'].sum() > weight:
        top_upcs = top_upcs.iloc[0:i-1]
        break

top_upcs = top_upcs[product_unique_identifier].unique()
logging.info('%d remaining out of the original %d.' % (len(top_upcs), len(prod_attr.index)))
########################################################

# Verify column names and data types
if Confirm:
    print('VERIFY PRODUCT ATTRIBUTE COLUMN NAMES AND DATA TYPES:')
    print(prod_attr.dtypes)
    input("Press Enter to continue...\r\n")

######################### END load product attributes #########################


########################### BEGIN load sales file(s) ##########################
print('Loading sales file(s)...\r\n')

# create list of all sales .csv files in the specified location so we can loop
# through them
all_files = glob.glob('.\Sales' + '/*.csv')

# Specify data types of fields that may/will be misinterpreted by read_csv
# << MANUAL_CUSTOMIZE >> #
sales = pd.concat((pd.read_csv(f, 
                               encoding= find_encoding(f),
                               dtype = 
                               {
                                'Geography': str,
                                'Product': str, 
                                'UPC 13': str, 
                                'UPC 10': str, 
                                'Retailer Store Number': str, 
                                'Dollar Sales': float,
                                'Unit Sales': float,
                                })
 for f in all_files))

# trim column names
sales.columns = sales.columns.str.strip()

# << MANUAL_CUSTOMIZE >> #
# rename any fields to be used for PCA and/or profiling (must include UPC or product identifier)
sales.rename(columns = 
             {
              #'Store Name': 'store_name',
              'Geography': 'store_nme',
              'Product': 'item_desc',
              'UPC 13': 'upc_13',
              'UPC 10': 'upc_10',
              'Retailer Store Number': 'store_nbr',

              'Dollar Sales': 'dol_cy',
              'Unit Sales': 'unit_cy',
              },
              inplace = True)
store_unique_identifier = 'store_nme'
# FOR BIG LOTS & HYVEE - drop UPC 13 and item_desc from sales files since they are already in product file and will duplicate measures
# keep measure that will be used for matching
sales.drop(columns=['upc_13', 'item_desc'], inplace = True)

# Verify column names and data types
if Confirm:
    print('VERIFY SALES COLUMN NAMES AND DATA TYPES:')
    print(sales.dtypes)
    input("Press Enter to continue...\r\n")

# delete list of sales files
del all_files

############################ END load sales file(s) ###########################


########################### BEGIN Load demographics ###########################

print('Loading demographics...\r\n')
demos = pd.read_csv('.\Demos\demos.csv')

############################ END Load demographics ############################



######################### BEGIN Load proscore centiles ########################
print('Loading proscores...\r\n')

proscores = pd.read_csv('./MVS_Files/avg_proscores_by_store.csv',
                        encoding= find_encoding('./MVS_Files/avg_proscores_by_store.csv'))

########################## END Load proscore centiles #########################


########################## BEGIN Load proscore lookup #########################
# DOUG this is the lookup to assign the 'dimension' string to each Proscore and
# can be removed once the HDInsight lookup table is available
print('Loading proscore lookup...\r\n')

# << MANUAL_CUSTOMIZE >> #
#make sure proscore_lookup.csv is updated
#KC replace with reference to SQL table


proscore_lookup = pd.read_csv('proscore_lookup.csv',
                                encoding= find_encoding('./proscore_lookup.csv'))    

########################### END Load proscore lookup ##########################
    




######################## BEGIN Clean and validate sales #######################

print('Cleaning sales data...\r\n')

# delete rows with dol_cy NaN or <= 0
# << MANUAL_CUSTOMIZE >> (make sure this is the correct measure name) >> #
sales.dropna(subset=['dol_cy'], inplace=True)
sales = sales[sales.dol_cy > 0]

# add dx_id from store_attr
sales = sales.merge(store_attr[[store_unique_identifier,'dx_id']])
#sales = sales.merge(store_attr[['store_nme','dx_id']])

# Enter the joining column if the columns were different names
# Enter the product fields to include for aggregation from prod_attr for either
# PCA or cluster profiles (product unique indentifier MUST be included)
# << MANUAL_CUSTOMIZE >> #
p_attr_to_include = [
                        #'retailer_name',not needed for dashboard
                        'item_desc', 
                        #'prdt_dollar_sales', not needed for dashboard
                        #'prdt_unit_sales',not needed for dashboard
                         'upc_13',
                         'upc_10',
                         'tsa_brand',
                         'tsa_manufacturer',
                         'tsa_parent',
                         'tsa_type',
                         'flavor_bucket',
                         'size',
                         'line_extension',	
                         'manufacturer',	
                         'parent_company',	
                         'price_tier',	
                         'serving_size',	
                         'sub_category',	
                         'take_home',	
                         'trademark'
]
                
# List the product attributes that will be included in cluster profiles, but
# NOT used in PCA/clustering (this will be a subset of p_attr_to_include, but 
# DO NOT include the product unique indentifier)
# << MANUAL_CUSTOMIZE >> #

p_attr_profiling_only = [
                        'item_desc', 
                         'upc_13',
                         'upc_10',
                         'tsa_brand',
                         'tsa_manufacturer',
                         'tsa_parent',
                         'tsa_type',
                         #'flavor_bucket',
                         'size',
                         'line_extension',	
                         'manufacturer',	
                         'parent_company',	
                         'price_tier',	
                         'serving_size',	
                         #'sub_category',	
                         #'take_home',	
                         'trademark'
]


# DOUG add ability to assign aliases to strings under each product aggregate, i.e. category, flavor_bucket, subcat, etc.

# add selected product attributes to the sales data
# << MANUAL_CUSTOMIZE >> #
full_df = pd.merge(sales, prod_attr[p_attr_to_include], on = product_unique_identifier)

# rename the sales measure we will use in clustering to the generic name "measure"
# << MANUAL_CUSTOMIZE >> #
full_df.rename(columns={'dol_cy': 'measure'}, inplace=True)

# Remove any sales fields we will not be using for PCA or cluster profiles
# DOUG need to find a way to make this dynamic.  It should include the store 
# unique ID, every product aggregate from p_attr_to_include, and measure
# order of measures is important for future code, keep in this order (dxid, product attributes, then measure)
# << MANUAL_CUSTOMIZE >> #
full_df = full_df[[
                    'dx_id',
                    'item_desc', 
                        #'prdt_dollar_sales', not needed for dashboard
                        #'prdt_unit_sales',not needed for dashboard
                         'upc_13',
                         'upc_10',
                         'tsa_brand',
                         'tsa_manufacturer',
                         'tsa_parent',
                         'tsa_type',
                         'flavor_bucket',
                         'size',
                         'line_extension',	
                         'manufacturer',	
                         'parent_company',	
                         'price_tier',	
                         'serving_size',	
                         'sub_category',	
                         'take_home',	
                         'trademark',
                         'measure'
                    ]]


# Merge full_df and store_attr on the store unique ID field to add the store aggregator
full_df = pd.merge(full_df, store_attr[s_attr_to_include], on = 'dx_id')

#manual customize
#columns should be in order of region, dx_id, feature_column, measure
#sends region to first
cols = list(full_df.columns)
cols = [cols[-1]] + cols[:-1]
full_df = full_df[cols]


#del p_attr_to_include - want to use for later

######################### END Clean and validate sales ########################


###############################################################################
################################# Process Data ################################
###############################################################################

########################### BEGIN Process Demographics #########################

print('Processing demographics...\r\n')

# add the region assignments from store_attr
demos = demos.merge(store_attr[s_attr_to_include])

# rename "households" to the generic name "measure"
demos.rename(columns = {'households': 'measure'}, inplace = True)

# move the region column to the first position so the DF is in the expected
# format to be processed in the Process Data section
# DOUG is there a more efficient way to do this?
cols = list(demos.columns)
cols = [cols[-1]] + cols[:-1]
demos = demos[cols]

# add a new column called 'pct_of_store' that is the percent of each store's
# HHs by demo group, i.e. 1 person, 2 Person, etc.
demos['pct_of_store'] = demos['measure'] / demos.groupby(['dx_id', 'demos'])['measure'].transform('sum')

# Add a % of region HHs by demo
demos['pct_of_region'] = demos.groupby(['region', 'demos', 'description'])['measure'].transform('sum') / demos.groupby(['region', 'demos'])['measure'].transform('sum')

# Add index of each store to its respective aggregate
# DOUG this will likely be omitted in the future, with z scores built off the %mix instead
demos['index'] = demos['pct_of_store'] / demos['pct_of_region'] * 100

# add column called 'dimension' that holds the name of the current demo
demos.rename(columns = {'demos': 'dimension'}, inplace = True)

# add column called 'level' that indicates if this is sales, demo, attitudinal, etc.
demos['level'] = 'demo'

# change the demo column name to 'value' so it is generic
demos.rename(columns = {'description': 'value'}, inplace = True)

# drop extra columns
demos = demos.drop(columns = ['zip5', 'zip3', 'lod'])
print('end Processing demographics...\r\n')
############################ END Process Dempgraphics #########################
   

############################## BEGIN Process Sales ############################

print('Processing sales...\r\n')

# Split the sales DF into mulitple DFs by sales aggregates and calculate sales 
# mix.  This works assuming 
#   1) store aggregator is in the first column
#   2) dx_id is in the second column,
#   3) sales measure is in the to last column.

# FOR loop should ONLY include the columns with product attributes on which we
# will aggregate sales
def get_sales_aggs_dict(df):
    sales_aggs_dict = {}
    for col_nbr in range(2, len(df.columns) - 1): 
        # name the new DF based in the current column name (aggregate string)
        lod = df.columns[col_nbr]
        
        # create the new DF
        lod_df = df[['region', 'dx_id', lod, 'measure']]
        
        # Sum the sales by store aggregate (first column), dx_id, and the current product aggregator
        lod_df = lod_df.groupby(['region', 'dx_id', lod], as_index = False).sum()
        
        # add a new column called 'pct_of_store' that is the percent of each store's
        # sales by prod agg, e.g. potato chips, corn chips, etc.
        lod_df['pct_of_store'] = lod_df['measure'] / lod_df.groupby('dx_id')['measure'].transform('sum')

        # Add a % of region sales by prod aggregate
        lod_df['pct_of_region'] = lod_df.groupby(['region', lod])['measure'].transform('sum') / lod_df.groupby('region')['measure'].transform('sum')
        
        # add an index column
        lod_df['index'] = (lod_df['pct_of_store'] / lod_df['pct_of_region']) * 100

        # add column called 'dimension' that holds the name of the current demo
        lod_df['dimension'] = lod
        
        # add column called 'level' that indicates if this is sales, demo, attitudinal, etc.
        lod_df['level'] = 'sales'
        
        # change the demo column name to 'value' so it is generic
        lod_df.rename(columns = {lod: 'value'}, inplace = True)

        # add dataframe to sales agg dictionary
        sales_aggs_dict[lod] = lod_df

    return sales_aggs_dict

sales_aggs_dict = get_sales_aggs_dict(full_df) # ALL DATA FOR PROFILING
#nmtms = ['ALANI NUTRITION LLC', 'CREAMLAND DAIRIES INC']
#sales_aggs_dict_top_upcs = get_sales_aggs_dict(full_df[(full_df['upc_10'].isin(top_upcs)) & (~full_df['manufacturer'].isin(nmtms))]) # FILTER OUT TAIL SKUS AND REMOVE NEW MEXICO BRANDS - MM 2022
sales_aggs_dict_top_upcs = get_sales_aggs_dict(full_df[(full_df[product_unique_identifier].isin(top_upcs))])

del full_df

############################### END Process Sales #############################


############################ BEGIN Process Proscores ##########################

print('Processing proscores...\r\n')

# convert the DF from wide to long
proscores = pd.melt(
        proscores,
        id_vars='dx_id',
        var_name='pro')

#rename the 'pro' and 'value' columns so they are standardized
proscores.rename(columns = {'value': 'measure', 'pro': 'value'}, inplace = True)

# add column called 'level' that indicates these are proscores
proscores['level'] =  'proscore'

# add column called 'dimension' that will be the proscore group
proscores = proscores.merge(proscore_lookup)

############################ END Process Proscores ##########################


###############################################################################
############################ Combine sales and demos ##########################
###############################################################################

# DOUG any columns with duplicate names are lost, e.g. 'unknown' - need to fix
# , but for now changing 'Unknown' lifestage to "Unknown Lifestage'
demos['value']= demos['value'].replace('Unknown', 'Unknown Lifestage') 
demos['value']= demos['value'].replace('Unknown', 'Unknown Ethnicity') 

def get_pca_df(dictionary):
    print('Creating pca_df...\r\n')

    # create new DF called 'pca_df' to use in PCA
    pca_df = pd.DataFrame()

    # add demos to pca_df
    pca_df = pca_df.append(demos)

    # add sales to pca_df
    for i in dictionary:
        pca_df = pca_df.append(dictionary[i])

    ################ BEGIN Remove any stores we are not clustering ################
    # add 'include_in_clusters' from store_attr
    pca_df = pca_df.merge(store_attr[['dx_id','include_in_clusters']])

    # drop all columns with 'include_in_clusters' = False
    pca_df = pca_df[pca_df.include_in_clusters == True]

    # drop column include_in_clusters
    pca_df.drop(['include_in_clusters'], axis = 1, inplace = True)
    return pca_df

# set filtered pca dataframe
pca_df = get_pca_df(sales_aggs_dict_top_upcs)

# recheck number of stores in pca_df (should be the same as store_attr with include_in_clusters = True)
print('We will be clustering ' + str(pca_df['dx_id'].nunique()) + ' stores\r\n') # count stores in pca_df
################# END Remove any stores we are not clustering #################

# create a copy of pca_df that will be used later to develop cluster profiles
profile_df = get_pca_df(sales_aggs_dict) # all data

# remove product agggregates rows from profile_df that are in p_attr_profiling_only
# since they should not be used for PCA
pca_df = pca_df[~pca_df.dimension.isin(p_attr_profiling_only)]

# Remove all columns not needed for PCA and clustering
# DOUG this is hard coded.  Needs to be dynamic.
pca_df = pca_df.drop(['region','measure','pct_of_store','pct_of_region','dimension', 'level'], axis = 1)

# Convert long to wide for PCA 
pca_df = pca_df.pivot_table(index = 'dx_id', columns = 'value', values = 'index')

# Create list of the fields in pca_df (this list is not used by anything, so may take this out)
pca_df_columns = pca_df.columns.values.tolist()

print('\r\n Created pca_df...')
created_pca_df_time = time.time()
print('\r\n Elapsed time from start to creating pca_df: ' + str(created_pca_df_time - start_time) + ' seconds')
print('\r\n Beginning PCA & clustering analysis... \r\n')
k_means_and_clustering_start_time = time.time()


###############################################################################
################################ PCA and kmeans ###############################
###############################################################################
# https://scikit-learn.org/stable/modules/generated/sklearn.cluster.KMeans.html
# https://www.kaggle.com/kkooijman/pca-and-kmeans

import matplotlib.pyplot as plt # Python plotting library
from sklearn.cluster import KMeans # KMeans clustering
from sklearn.decomposition import PCA # Principal Component Analysis module
import seaborn as sns # plotting library
from sklearn.preprocessing import StandardScaler

# NaN values must be removed before proceeding (DOUG verify this is needed)
pca_df = pca_df.fillna(value=0, axis=1)

#pca_df.describe()
list(pca_df)

# convert to numpy array of z scores
# https://scikit-learn.org/stable/modules/generated/sklearn.preprocessing.StandardScaler.html#sklearn.preprocessing.StandardScaler.fit_transform
pca_df_std = StandardScaler().fit_transform(pca_df.values)

# Calculating eigenvectors and eigenvalues of covariance matrix
# https://docs.scipy.org/doc/numpy-1.15.1/reference/generated/numpy.linalg.eigvals.html#numpy.linalg.eigvals
# mean_vec = np.mean(pca_df_std, axis=0) # calculate the mean of columns
# T is the numpy transpose function
cov_mat = np.cov(pca_df_std.T) # estimate the covariance matrix

# DOUG they are sorted here, so how do I link back to the original column names so we know what vectors are retained?
eig_vals, eig_vecs = np.linalg.eig(cov_mat) # Compute the eigenvalues and right eigenvectors of a square array.

# Rank the eigenvalues from highest to lowest and choose the top k eigenvectors.
# DOUG they are already ranked so why is this needed?
# Make a list of (eigenvalue, eigenvector) tuples
eig_pairs = [ (np.abs(eig_vals[i]),eig_vecs[:,i]) for i in range(len(eig_vals))]

# DOUG these are already sorted, so I don't think this is needed
# Sort the (eigenvalue, eigenvector) tuples from high to low
eig_pairs.sort(key = lambda x: x[0], reverse= True)

# The explained variance tells us how much information (variance) can be 
# attributed to each of the principal components
tot = sum(eig_vals)
var_exp = [(i/tot)*100 for i in sorted(eig_vals, reverse=True)] # Individual explained variance
cum_var_exp = np.cumsum(var_exp) # Cumulative explained variance

# PLOT OUT THE EXPLAINED VARIANCES SUPERIMPOSED 
print('Plotting explained variance...\r\n')
plt.figure(figsize=(10, 8))
plt.bar(range(len(var_exp)), var_exp, alpha=0.3333, align='center', label='individual explained variance', color = 'g')
plt.step(range(len(cum_var_exp)), cum_var_exp, where='mid',label='cumulative explained variance')
plt.ylabel('Explained variance ratio')
plt.xlabel('Principal components')
plt.legend(loc='best')
# DOUG this needs to save to the Output folder and needs to be higher resolution
# << MANUAL_CUSTOMIZE >> #
#UPDATE_FILE_NAMES
plt.savefig(dirpath + '/Output/cumulative variance of PCs.png')

# We want to choose the number of X eigenvectors that account for a specified
# cumulative variance.
# A projection matrix can be used to transform data onto the new subspace. It 
# basically is just a matrix of our concatenated top X eigenvectors.

# We'll reduce our original high-dimensional space to a X-dimensional subspace 
# by choosing the top X eigenvectors to construct our eigenvector matrix W .

# assign the total number of eigenvectors to ttl_eig_vectors
ttl_eig_vectors = len(eig_vals)

# set the cumulative variance threshold percentage as an integer
cum_var_thresh = 85

# eig_vec_count is the number of eig vectors needed to reach the threshold cum 
# variance of cum_var_thresh (argmax returns the index so add one since it is 
# zero-based)
eig_vec_count = np.argmax(cum_var_exp > cum_var_thresh) + 1
print('We are including ' + str(eig_vec_count) + ' of the original ' + str(ttl_eig_vectors) + ' dimensions to reach ' + str(cum_var_thresh) + '% cumulative variance explained\r\n') # count stores in pca_df
if Confirm:input("Press Enter to continue...\r\n")

###################### BEGIN PCA and sum of squares plot  #####################

# run PCA on standardized data, only include the first eig_vec_count dimensions
pca = PCA(n_components=eig_vec_count, svd_solver='full')
x_select_d = pca.fit_transform(pca_df_std) #numpy array

# Run clustering options (K) 2 to nClust using x_select_d and generate sum of squared errors plot
nClust = 10  # 10 cluster options

Sum_of_squared_distances = []
K = range(2,nClust + 1)
for k in K:
    km = KMeans(n_clusters=k)
    km = km.fit(x_select_d)
    label = km.labels_ # index of the cluster each sample belongs to
    Sum_of_squared_distances.append(km.inertia_) # Sum of squared distances of samples to their closest cluster center.

# to see kmeans parameters...
print(km)

# clear the plot canvas of residuals from the previous plot.
plt.clf()

# Generate sum of squares plot
print('Plotting sum of squares...\r\n')
plt.plot(K, Sum_of_squared_distances, 'bx-')
plt.xlabel('Clusters')
plt.ylabel('Sum_of_squared_distances')
plt.title('Elbow Method For Optimal Clusters')
# DOUG Y axis label is getting cut off in the png file
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
plt.savefig(dirpath + '/Output/Sum_of_squared_distances.png')

del k

# clear the plot canvas of residuals from the previous plot.
plt.clf()

####################### END PCA and sum of squares plot  ######################



###############################################################################
# KMeans clustering with number of clusters = n_clusters, using the PCs that 
# get us to 85% of cum var (x_select_d)
# from collections import Counter

# set the number of cluster scenarios we want to run (this typically will not change)
# << MANUAL_CUSTOMIZE >> #
n_clusters = [2,3,4,5,6,7,8,9,10]


# create a dummy DF to hold all cluster options' profiles
all_profiles = pd.DataFrame()
cluster_summaries = pd.DataFrame()
space_team_data = pd.DataFrame()
cluster_assignments = pd.DataFrame()
cluster_pc_plot = pd.DataFrame()

# Sum total sales by store to be used later.  This uses the first DF referenced
# in the sales_aggs list to retrieve sales.
store_sales_temp = sales_aggs_dict[product_unique_identifier].groupby(['dx_id'], as_index = False)['measure'].agg('sum')

for clust in n_clusters:
    kmeans = KMeans(clust, random_state = 10, max_iter = 2000)
    
    # run keans for the current cluster scenario and return the cluster assignments
    X_clustered = kmeans.fit_predict(x_select_d)

    # Create temp dataframe clust_sum_temp that will hold store counts and 
    # sales by cluster.  This converts the cluster assignments DF and moves the
    # store numbers from index values to the first column of the DF.
    clust_sum_temp = pd.DataFrame(X_clustered, index = pca_df.index).reset_index()
    
    # name the column containing the cluster assignment from '0' to 'cluster'
    clust_sum_temp.rename(columns = {0: 'cluster'}, inplace = True)
    
    # increment the cluster numbers so they don't start at 0 (more readable)
    clust_sum_temp['cluster'] += 1
    
    # Collect data to plot each cluster option's PC plots
    cluster_pc_plot_temp = pd.DataFrame()
    cluster_pc_plot_temp['PC1'] = x_select_d[:,0]
    cluster_pc_plot_temp['PC2'] = x_select_d[:,1]
    cluster_pc_plot_temp = cluster_pc_plot_temp.join(clust_sum_temp, how = 'outer')
    
    # add column 'Cluster Option' specifying the cluster option
    cluster_pc_plot_temp['Cluster Option'] = str(clust) + ' clusters'
    
    # add the current cluster option's PC plot data to the main DF to be
    # printed later
    cluster_pc_plot = cluster_pc_plot.append(cluster_pc_plot_temp, ignore_index = True)
    
    # merge the cluster assignments with store-level, upc-level sales
    clust_sum_temp_space = clust_sum_temp.merge(sales)
    
    # drop the store_nbr before summing at cluster level since it is a numerical 
    # datatype (this will ONLY need to be changed if store_nbr is not the store
    # primary key)
    # << MANUAL_CUSTOMIZE >> #
    #clust_sum_temp_space.drop(['store_nbr'], inplace = True, axis = 1)
    clust_sum_temp_space.drop([store_unique_identifier], inplace = True, axis = 1)
    
    # sum all sales fields
    clust_sum_temp_space = clust_sum_temp_space.groupby(['cluster',product_unique_identifier], as_index = False).sum()
    
    # << MANUAL_CUSTOMIZE >> #
    # add selected product attributes from the product attributes that need to
    # be included for the space team
    # added all product attributes
    clust_sum_temp_space = clust_sum_temp_space.merge(prod_attr[p_attr_to_include])
    
    
    # add column 'Cluster Option' specifying the cluster option
    clust_sum_temp_space['Cluster Option'] = str(clust) + ' clusters'
    
    # add each cluster UPC sales to space_team_data
    space_team_data = space_team_data.append(clust_sum_temp_space, ignore_index = True)
    
    # add store attributes to the cluster assignments and save to csv
    clust_sum_temp_csv = clust_sum_temp.merge(store_attr, on= 'dx_id')
    
    # << MANUAL_CUSTOMIZE >> #
    # remove any store attributes not needed for the store-level file with
    # cluster assignments. This should include:
    # dx_id
    # cluster assignment
    # retailer store primary key
    # address
    # city
    # state
    # postal code
    # latitude
    # longitude
    # cluster option
    # any other custom attributes
    # Note: There should be nothing to change if all unneeded store attributes
    # provided by the customer were removed from the store attribute csv file.
    #clust_sum_temp_csv.drop(['include_in_clusters','btg_store_nbr'],  inplace= True, axis = 1)
    #clust_sum_temp_csv.drop(['include_in_clusters','tdlinx'],  inplace= True, axis = 1)
    
    # add column 'Cluster Option' specifying the cluster option
    clust_sum_temp_csv['Cluster Option'] = str(clust) + ' clusters'
    
    # add the current cluster option's cluster assignments to main DF to be
    # printed later
    cluster_assignments = cluster_assignments.append(clust_sum_temp_csv, ignore_index = True)
    
    # create DF of dx_id, cluster assignment, sales measure
    clust_sum_temp2 = clust_sum_temp.merge(store_sales_temp[['dx_id','measure']])
    
    # sum sales and count stores by cluster
    clust_sum_temp2 = pd.DataFrame(clust_sum_temp2.groupby('cluster')['measure'].agg(['count','sum'])).reset_index()
    
    # add column 'Cluster Option' specifying the cluster option
    clust_sum_temp2['Cluster Option'] = str(clust) + ' clusters'
    
    # add each cluster option's store counts and total sales to cluster_summaries
    cluster_summaries = cluster_summaries.append(clust_sum_temp2, ignore_index = True)

    ############################ Build cluster profile ########################
    
    # profile_df has all the product aggregates, associated sales, demos, and 
    # assocaited households by store
    
    # add the cluster assignments to profile_df and make a new DF
    profile_df_temp = profile_df.merge(clust_sum_temp, on= 'dx_id')

    # Remove all columns not needed to develop cluster prfiles
    profile_df_temp = profile_df_temp[['level','cluster','dimension','value','measure']]

    # Sum measures (HHs and sales) by cluster, dimension, and value to remove duplication from store records
    profile_df_temp = profile_df_temp.groupby(['level','cluster','dimension','value'], as_index = False).sum()

    # add a new column called 'pct_of_cluster' that is the percent of each
    # cluster's measure by demo and sales groups
    profile_df_temp['pct_of_cluster'] = profile_df_temp['measure'] / profile_df_temp.groupby(['cluster','dimension'])['measure'].transform('sum')
    
    # add a new column called 'pct_of_all' that is the percent of all clusters'
    # measure by demo and sales groups
    profile_df_temp['pct_of_all'] = profile_df_temp.groupby(['dimension','value'])['measure'].transform('sum') / profile_df_temp.groupby(['dimension'])['measure'].transform('sum')

    ##### BEGIN Calculate proscore indices and add them to profile_df_temp ####
    
    # add the cluster assignments to proscores and make a new DF
    proscores_temp = proscores.merge(clust_sum_temp, on= 'dx_id')
    
    # Remove all columns not needed to develop cluster prfiles
    proscores_temp = proscores_temp[['level','cluster','dimension','value','measure']]
    
    # Average the centiles by cluster, dimension, and value to remove duplication 
    # from store records.  Save as new DF to retain store-level records when the
    # all-store average is calculated.
    proscores_temp_cluster = proscores_temp.groupby(['level', 'cluster', 'dimension', 'value'], as_index = False).mean()

    # Average the centiles across all stores
    proscores_temp_all = proscores_temp.groupby(['level', 'dimension', 'value'])['measure'].mean().reset_index()
    proscores_temp_all.rename(columns = {'measure': 'measure_all'}, inplace = True)

    # add all store centile averages to proscores_temp_cluster
    proscores_temp_cluster = proscores_temp_cluster.merge(proscores_temp_all[['value','measure_all']])
    
    # add index column
    proscores_temp_cluster['index'] = proscores_temp_cluster['measure'] / proscores_temp_cluster['measure_all'] * 100
    
    # add placeholder columns so the proscore rows can be appended to all_profiles
    proscores_temp_cluster['pct_of_cluster'] = ''
    proscores_temp_cluster['pct_of_all'] = ''
    
    # add each cluster option's profiles to all_profiles
    proscores_temp_cluster['cluster_option'] = str(clust) + ' clusters'
    
    # reorder the columns to load into all_profiles
    proscores_temp_cluster = proscores_temp_cluster[['level','cluster','dimension','value','measure','pct_of_cluster','pct_of_all','index','cluster_option']]
    
    ##### END Calculate proscore indices and add them to profile_df_temp ####

    # add index column of cluster %mix / all clusters' %mix
    profile_df_temp['index'] = profile_df_temp['pct_of_cluster'] / profile_df_temp['pct_of_all'] * 100

    # add column called 'cluster_option' to indicate which cluster option this is
    profile_df_temp['cluster_option'] = str(clust) + ' clusters'

    # append cluster proscore indices to the rest of the cluster profile info
    profile_df_temp = profile_df_temp.append(proscores_temp_cluster, ignore_index = True)

    # add each cluster option's profiles to all_profiles
    all_profiles = all_profiles.append(profile_df_temp, ignore_index = True)

# restrict store-level profile_df to only demos, needed columns, then save
profile_df.drop(
        ['region',
         'measure',
         'pct_of_region'], axis=1, inplace=True)
profile_df = profile_df[profile_df.level=='demo']
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
profile_df.to_csv('./Output/store_demos.csv', index=False)

# save the cluster profles
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
all_profiles.to_csv('./Output/cluster_profiles.csv', index=False)

# save the PC plot data to csv
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
cluster_pc_plot.to_csv('./Output/cluster_pc_plot.csv', index=False)

# save the space team sales to deliver to that team
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
space_team_data.to_csv('./Output/cluster_upc_sales.csv', index=False)

# rename the 'count' and 'sum' columns of cluster_summaries
cluster_summaries.rename(columns={
        'count': 'number of stores',
        'sum': 'sales'
        }, inplace=True)

# save the cluster summaries (# stores, ttl sales)
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
cluster_summaries.to_csv('./Output/cluster_summaries.csv', index=False)

# save the clsuter assignments and store attributes to csv
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
cluster_assignments.to_csv('./Output/Cluster_assignments.csv', index = False)

del clust

# delete the DFs in sales_aggs_dict
del sales_aggs_dict
del sales_aggs_dict_top_upcs

# =============================================================================
# Input in Tableau
# =============================================================================
print('Fill Tableau with files')
tableau_file = 'Clustering_Default.twbx'
#delete the folders first in case something was left in there
# =============================================================================
with ZipFile(tableau_file, 'a') as zipped:
    del_tf = list(filter(lambda f: not f.endswith('twb'), zipped.namelist()))

for f in del_tf:
    cmd = ['zip', '-d', tableau_file, f]
    subprocess.run(cmd, shell = True, check = True)
# =============================================================================

#open the Tableau file as a zipped file in append "a"
with ZipFile(tableau_file, 'a') as zipped:
    #UPDATE_FILE_NAMES
    # << MANUAL_CUSTOMIZE >> #
    #cluster_summaries
    #write the csv file from the profile to the Tableau file
    zipped.write("./Output/cumulative variance of PCs.png", arcname="/Image/cumulative variance of PCs.png")
    zipped.write("./Output/Sum_of_squared_distances.png", arcname="/Image/Sum_of_squared_distances.png")
    zipped.write("./Output/Cluster_assignments.csv", arcname="/Data/Output/Cluster_assignments.csv")
    zipped.write("./Output/cluster_pc_plot.csv", arcname="/Data/Output/cluster_pc_plot.csv")
    zipped.write("./Output/cluster_profiles.csv", arcname="/Data/Output/cluster_profiles.csv")
    zipped.write("./Output/cluster_summaries.csv", arcname="/Data/Output/cluster_summaries.csv")
    zipped.write("./Output/cluster_upc_sales.csv", arcname="/Data/Output/cluster_upc_sales.csv")
    zipped.write("./Output/store_demos.csv", arcname="/Data/Output/store_demos.csv")

# =============================================================================
# Create the tsne file
# =============================================================================
#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
np.savetxt("pca_filtered.tsv", x_select_d, delimiter="\t")

#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
np.savetxt("pca.tsv", pca_df_std, delimiter="\t")

#get metadata for dx_id and cluster
df_piv = pd.pivot_table(cluster_assignments, values='cluster', index=['dx_id'],columns=['Cluster Option']).reset_index()

#df_tmp_store = store_attr[['dx_id','store_nbr', 'region']]
df_tmp_store = store_attr[['dx_id',store_unique_identifier, 'region']]

df_piv = pd.merge(df_piv,df_tmp_store, how='left', on='dx_id')

#UPDATE_FILE_NAMES
# << MANUAL_CUSTOMIZE >> #
df_piv.to_csv('meta.tsv', sep='\t', index=False, header=True)

print('Done')
k_means_and_clustering_end_time = time.time()
print('\r\n Elapsed time from creation of PCA DF to PCA & clustering completion: ' + str(k_means_and_clustering_end_time - k_means_and_clustering_start_time) + ' seconds')
print('Total elapsed time: ' + str(time.time() - start_time) + ' seconds')