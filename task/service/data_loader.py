import pandas as pd
import os

from db.operations import create_table_from_df, insert_data_executemany, fetch_all_data

def load_csv_to_table(csv_file):
    table_name = os.path.splitext(os.path.basename(csv_file))[0]
    df = pd.read_csv(csv_file)

    create_table_from_df(table_name, df)
    insert_data_executemany(table_name, df)

    return table_name
