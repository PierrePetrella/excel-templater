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
insert_tag = get_recipe_config().get('input_tag', "DATASET")
output_file_name = get_recipe_config().get('output_file_name',input_folder.list_paths_in_partition()[0].split(".")[0] )

row_max = 50
col_max = 50

### Look for insert_tags and insert the associated dataset ###
wb = read_wb_from_managed_folder(input_folder)

for sheet_name in wb.sheetnames:
    ws = wb[sheet_name]
    tags = find_tags_in_ws(ws, insert_tag, row_max, col_max)
    for tag in tags:
        if len(tag)!=3:
            raise Exception("Tag {} is not well formatted".format(tag))
        dataset_name = tag[0].split(".")[1]
        dataset = dataiku.Dataset(dataset_name)
        df = dataset.get_dataframe()
        populate_table_in_ws(df, ws, tag[1], tag[2])
write_wb_to_managed_folder(wb,output_folder, output_file_name + ".xlsx")

### OLD - method using the metadata mapping dataset ###
#wb = read_wb_from_managed_folder(input_folder)
## For each row of the metadata Table
#for index, row in input_metadata_df.iterrows():
#    sheet_name = row["sheet_name"]
#    dataset_name = row["dataset_name"]
#    dataset = dataiku.Dataset(dataset_name)
#    df = dataset.get_dataframe()
#    if sheet_name in wb.sheetnames:
#        ws = wb[sheet_name]
#        tags = find_tags_in_ws(ws, insert_tag, row_max, col_max)
#        for tag in tags:
#            populate_table_in_ws(df, ws, tag[1], tag[2])
#    else:
#        raise Exception("Sheet {} doesn't exist".format(sheet_name))
#write_wb_to_managed_folder(wb,output_folder, output_file_name + ".xlsx")