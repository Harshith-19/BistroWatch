import psycopg2
from connection import get_db_connection
import csv

def timezone_data_dump():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        with open('CSV/timezone.csv', 'r', newline='') as csvfile:
            csv_reader = csv.reader(csvfile)
            next(csv_reader)
            for row in csv_reader:
                store_id, timezone_str = row
                sql_insert = "INSERT INTO timezone (store_id, timezone_str) VALUES (%s, %s)"
                cursor.execute(sql_insert, (store_id, timezone_str))
            conn.commit()
        return {"message": "Timezone data inserted successfully"}
    except psycopg2.Error as e:
        return {"error": f"Error inserting timezone data: {e}"}
    finally:
        cursor.close()
        conn.close()

result = timezone_data_dump()
print(result)
