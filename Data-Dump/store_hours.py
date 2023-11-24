import psycopg2
from connection import get_db_connection
import csv

def menu_hours_data_dump():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        with open('CSV/menu_hours_data.csv', 'r', newline='') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)
            for row in csv_reader:
                store_id, day, start_time_local, end_time_local = row
                sql_insert = "INSERT INTO menu_hours (store_id, day, start_time_local, end_time_local) VALUES (%s, %s, %s, %s)"
                cursor.execute(sql_insert, (store_id, day, start_time_local, end_time_local))
            conn.commit()
        return {"message": "Menu hours data inserted successfully"}
    except psycopg2.Error as e:
        return {"error": f"Error inserting menu hours data: {e}"}
    finally:
        cursor.close()
        conn.close()

result = menu_hours_data_dump()
print(result)
