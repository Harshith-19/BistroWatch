from connection import get_db_connection
import psycopg2
from Helpers.constants import *
from datetime import datetime, timezone
import threading
import os
import csv


def generate_report():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        current_utc_time = datetime.utcnow().replace(tzinfo=timezone.utc)

        cursor.execute("INSERT INTO reports (status, created_at) VALUES (%s, %s) RETURNING report_id", (RUNNING_STATUS, current_utc_time))
        report_id = cursor.fetchone()[0]
        conn.commit()

        report_thread = threading.Thread(
            target=report_generation_thread, args=(report_id,))
        report_thread.start()
        return report_id
    
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating report entry: {e}")
        return None
    
    finally:
        cursor.close()
        conn.close()


def report_generation_thread(report_id):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("SELECT DISTINCT store_id FROM store_status")
        store_ids = [row[0] for row in cursor.fetchall()]

        folder_path = os.path.join(os.path.dirname(__file__), "reports")
        os.makedirs(folder_path, exist_ok=True)
        report_name = "report-"+str(report_id)+".csv"
        report_filename = os.path.join(folder_path, report_name)

        with open(report_filename, 'w', newline='') as csvfile:
            fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for store_id in store_ids:
                metrics = report_generation_store(store_id)
                writer.writerow({'store_id': store_id, **metrics})
        
        current_utc_time = datetime.utcnow().replace(tzinfo=timezone.utc)
        cursor.execute("UPDATE reports SET status = %s, generated_at = %s, name = %s WHERE report_id = %s", (COMPLETE_STATUS, current_utc_time, report_name, report_id))
        conn.commit()

    except psycopg2.Error as e:
        conn.rollback()
        print(f"Error creating report entry: {e}")
    
    finally:
        cursor.close()
        conn.close()


def report_generation_store(store_id):
    pass