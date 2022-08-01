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
query_tag = get_recipe_config().get('SQL_Tag', "SQL_Tag")
cnx_name = get_recipe_config().get('sql_cnx', "")
output_file_name = get_recipe_config().get('output_file_name',input_folder.list_paths_in_partition()[0].split(".")[0] )


row_max = 50
col_max = 50
# Get haddle on the input workbook
wb = read_wb_from_managed_folder(input_folder)

for sheet_name in wb.sheetnames:
    ws = wb[sheet_name]
    tags = find_tags_in_ws(ws,query_tag, row_max, col_max)
    for tag in tags:
        raw_query = tag[0]
        query = parse_query_from_raw_query(raw_query)
        df = get_df_from_query(cnx_name,query)
        #ws = populate_table_in_ws(df, ws,tag[1], tag[2])
        start_row = tag[1]
        start_col = tag[2]
        df_np = df.values()
        for row_num in range(df.shape[0]):
            for col_num in range (df.shape[1]):
                ws.cell(row = (row_num + start_row), column = (col_num +start_col)).value =  df_np[row_num][col_num]

file_name = output_template_name
write_wb_to_managed_folder(wb,output_folder, output_file_name)

