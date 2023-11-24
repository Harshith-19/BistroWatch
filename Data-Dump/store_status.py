import psycopg2
from connection import get_db_connection
import csv

def store_data_dump():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        with open('CSV/store status.csv', 'r', newline='') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)
            for row in csv_reader:
                store_id, status, timestamp_utc = row
                sql_insert = f"INSERT INTO store_status (store_id, status, timestamp_utc) VALUES (%s, %s, %s)"
                cursor.execute(sql_insert, (store_id, status, timestamp_utc))
            conn.commit()
        return {"message": "Data inserted successfully"}
    except psycopg2.Error as e:
        return {"error": f"Error inserting data: {e}"}
    finally:
        cursor.close()
        conn.close()

result = store_data_dump()
print(result)