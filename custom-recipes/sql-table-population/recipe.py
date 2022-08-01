import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from helper_functions import *

### Get handles in INPUT and OUTPUT 
# Get handle on input folders
input_folder_name = get_input_names_for_role('input_folder')[0]
input_folder = dataiku.Folder(input_folder_name)

# Get handle on output dataset name to feed to the "COPY" query
output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# Retrieve mandatory user-defined parameters
SQL_Tag = get_recipe_config().get('SQL_Tag', "SQL_Tag")
cnx_name = get_recipe_config().get('sql_cnx', "")
dataset_id = get_recipe_config().get('dataset_id', "dataset_id")


row_max = 50
col_max = 50
# Get haddle on the input workbook
wb = read_wb_from_managed_folder(input_folder)

for sheet_name in wb.sheetnames:
    ws = wb[sheet_name]
    tags = find_tags_in_ws(ws,row_max, col_max)
    for tag in tags:
        raw_query = tag[0]
        query = parse_query_from_raw_query(raw_query)
        df = get_df_from_query(cnx_name,query)
        ws = populate_table_in_ws(df, ws,tag[1], tag[2])

file_name = output_template_name
write_wb_to_managed_folder(wb,output_folder, file_name)

