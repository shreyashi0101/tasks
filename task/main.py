from service.data_loader import load_csv_to_table
from db.operations import fetch_all_data


csv_file = "students.csv"
table_name = load_csv_to_table(csv_file)

columns, rows = fetch_all_data(table_name)

print(f"\nTable: {table_name}")
print(", ".join(columns))
print("-" * 50)
for row in rows:
    print(row)

print(f"\nTable '{table_name}' created and printed successfully.")

