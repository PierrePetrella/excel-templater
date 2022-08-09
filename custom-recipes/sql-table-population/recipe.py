import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from dataiku.customrecipe import *
from helper_functions import *

### Get handles in INPUT and OUTPUT 
# Get handle on input folders
input_folder_name = get_input_names_for_role('input_folder')[0]
input_folder = dataiku.Folder(input_folder_name)

# Get handle on output folder
output_folder_name = get_output_names_for_role('output_folder')[0]
output_folder = dataiku.Folder(output_folder_name)

# Retrieve mandatory user-defined parameters
sql_tag = get_recipe_config().get('SQL_Tag', "SQL_Tag")
cnx_name = get_recipe_config().get('sql_cnx', "")
output_file_name = get_recipe_config().get('output_file_name',input_folder.list_paths_in_partition()[0].split(".")[0] )


row_max = 50
col_max = 50
# Get haddle on the input workbook
wb = read_wb_from_managed_folder(input_folder)

def poluate_wb_from_sql(wb,cnx_name,sql_tag,row_max=50, col_max=50):
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        tags = find_tags_in_ws(ws,sql_tag, row_max, col_max)
        for tag in tags:
            raw_query = tag[0]
            query = raw_query.replace(sql_tag,"")
            query = insert_project_variables_in_query(query)
            df = get_df_from_query(cnx_name,query)
            ws = populate_table_in_ws(df, ws,tag[1], tag[2])

poluate_wb_from_sql(wb,cnx_name,sql_tag,row_max=50, col_max=50)
  
write_wb_to_managed_folder(wb,output_folder, output_file_name + ".xlsx")

