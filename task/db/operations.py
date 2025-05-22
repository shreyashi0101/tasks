from db.connection import get_db_connection
import psycopg2

def create_table_from_df(table_name, df):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(f"DROP TABLE IF EXISTS {table_name};")

    columns = ", ".join([f"{col.replace(' ', '_')} TEXT" for col in df.columns])
    create_stmt = f"CREATE TABLE {table_name} ({columns});"
    cur.execute(create_stmt)

    conn.commit()
    cur.close()
    conn.close()

def insert_data_executemany(table_name, df, batch_size=100):
    conn = get_db_connection()
    cur = conn.cursor()

    columns = [col.replace(' ', '_') for col in df.columns]
    placeholders = ', '.join(['%s'] * len(columns))
    columns_formatted = ', '.join(columns)
    insert_query = f"INSERT INTO {table_name} ({columns_formatted}) VALUES ({placeholders})"

    data = [tuple(row) for row in df.itertuples(index=False)]

    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        cur.executemany(insert_query, batch)

    conn.commit()
    cur.close()
    conn.close()

def fetch_all_data(table_name):
    conn = get_db_connection()
    cur = conn.cursor()

    cur.execute(f"SELECT * FROM {table_name};")
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]

    cur.close()
    conn.close()

    return columns, rows
