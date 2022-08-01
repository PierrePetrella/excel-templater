import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from helper_functions import *

### Get handles in INPUT and OUTPUT 
# Get handle on input folders
input_folder_name = get_input_names_for_role('input_folder')[0]
input_folder = dataiku.Folder(input_folder_name)

input_metadata_name = get_input_names_for_role('input_metadata')[0]
input_metadata_dataset = dataiku.Dataset(input_folder_name)
input_metadata_df = input_metadata_dataset.get_dataframe()





# Get handle on output dataset name to feed to the "COPY" query
output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# Retrieve mandatory user-defined parameters
query_tag = get_recipe_config().get('SQL_Tag', "SQL_Tag")
cnx_name = get_recipe_config().get('sql_cnx', "")
output_file_name = get_recipe_config().get('output_file_name',input_folder.list_paths_in_partition()[0].split(".")[0] )