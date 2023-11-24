from connection import get_db_connection
import psycopg2
from Helpers.constants import *
from Helpers.helpers import *
from datetime import datetime, timezone, timedelta
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
                metrics = report_generation_store(store_id, )
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


def report_generation_store(store_id, cursor):
    try:
        metrics = {
            "store_id" : store_id,
            "uptime_last_hour" : 0,
            "uptime_last_day" : 0,
            "uptime_last_week" : 0,
            "downtime_last_hour" : 0,
            "downtime_last_day" : 0,
            "downtime_last_week" : 0
        }

        last_hour_threshold = CURRENT_TIME - timedelta(hours=1)
        last_day_threshold = CURRENT_TIME - timedelta(days=1)

        cursor.execute("""
            SELECT *,
                CASE 
                    WHEN timestamp_utc >= %s THEN TRUE 
                    ELSE FALSE 
                END AS in_last_hour,
                CASE 
                    WHEN timestamp_utc BETWEEN %s AND %s THEN TRUE 
                    ELSE FALSE 
                END AS in_last_day
            FROM store_status
            WHERE store_id = %s
            AND timestamp_utc BETWEEN %s - interval '1 week' AND %s
        """, (last_hour_threshold, last_day_threshold, CURRENT_TIME, store_id, CURRENT_TIME, CURRENT_TIME))   
        store_reports = cursor.fetchall()
        store_reports = sorted(store_reports, key=lambda x: x[2], reverse=True)

        cursor.execute("SELECT timezone_str FROM timezone WHERE store_id = %s", (store_id,))
        timezone = cursor.fetchone()
        if timezone:
            timezone = timezone[0]
        else:
            timezone = DEFAULT_TIMEZONE
        
        cursor.execute("SELECT * FROM menu_hours WHERE store_id = %s", (store_id,))
        business_hours = cursor.fetchall()

        local_time = get_local_time(CURRENT_TIME, timezone)

        for index, entry in enumerate(store_reports):
            report_time = get_local_time(store_reports[index][2], timezone)
            report_day = get_local_day(store_reports[index][2], timezone)
            if (index > 0):
                report_frame_end = store_reports[index-1][2]
            else:
                report_frame_end = local_time
            
            local_start_time, local_end_time = get_local_hours(business_hours)

    except psycopg2.Error as e:
        print(f"Error: {e}")