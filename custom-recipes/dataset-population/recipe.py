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

input_datasets = get_input_names_for_role('input_datasets')


# Get handle on output folder
output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# Retrieve mandatory user-defined parameters
input_tag = get_recipe_config().get('input_tag', "DATASET")
output_file_name = get_recipe_config().get('output_file_name',input_folder.list_paths_in_partition()[0].split(".")[0] )

row_max = 50
col_max = 50
# Get handle on wb
wb = read_wb_from_managed_folder(template_folder)
# For each row of the metadata Table
for index, row in metadata_df.iterrows():
    sheet_name = row["sheet_name"]
    dataset_name = row["dataset_name"]
    dataset = dataiku.Dataset(dataset_name)
    df = dataset.get_dataframe()
    if sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        tags = find_tags_in_ws(ws, insert_tag, row_max, col_max)
        for tag in tags:
            populate_table_in_ws(df, ws, tag[1], tag[2])
    else:
        raise Exception("Sheet {} doesn't exist".format(sheet_name))

write_wb_to_managed_folder(wb,output_folder, output_file_name + ".xlsx")