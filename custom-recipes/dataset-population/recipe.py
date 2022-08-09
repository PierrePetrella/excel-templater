import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from helper_functions import *

### Get handles in INPUT and OUTPUT 
# Get handle on input folders
input_folder_name = get_input_names_for_role('input_folder')[0]
input_folder = dataiku.Folder(input_folder_name)

input_datasets = get_input_names_for_role('input_datasets')


# Get handle on output folder
output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# Retrieve mandatory user-defined parameters
dataset_tag = get_recipe_config().get('dataset_tag', "DATASET")
output_file_name = get_recipe_config().get('output_file_name',input_folder.list_paths_in_partition()[0].split(".")[0] )

row_max = 50
col_max = 50

### Look for insert_tags and insert the associated dataset ###
wb = read_wb_from_managed_folder(input_folder)
        
popluate_wb_from_dataset(wb, dataset_tag, row_max, col_max)

write_wb_to_managed_folder(wb,output_folder, output_file_name + ".xlsx")
