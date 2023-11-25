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

        # An new entry is made into reports table with running status and corresponding report ID generated is fetched
        cursor.execute("INSERT INTO reports (status, created_at) VALUES (%s, %s) RETURNING report_id", (RUNNING_STATUS, current_utc_time))
        report_id = cursor.fetchone()[0]
        conn.commit()

        # A new thread is started to generate the report
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

        # A list of all store IDs whose activity status is present is fetched
        cursor.execute("SELECT DISTINCT store_id FROM store_status")
        store_ids = [row[0] for row in cursor.fetchall()]

        # A CSV file is made where the metrics of each store is to be stored
        folder_path = os.path.join(os.path.dirname(__file__), "reports")
        os.makedirs(folder_path, exist_ok=True)
        report_name = "report-"+str(report_id)+".csv"
        report_filename = os.path.join(folder_path, report_name)

        with open(report_filename, 'w', newline='') as csvfile:
            fieldnames = ['store_id', 'uptime_last_hour', 'uptime_last_day', 'uptime_last_week', 'downtime_last_hour', 'downtime_last_day', 'downtime_last_week']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for store_id in store_ids:
                # For each store corresponding metrics are calculated and stored in the CSV file
                metrics = report_generation_store(store_id, cursor)
                writer.writerow({'store_id': store_id, **metrics})
        
        # Since the report generation is complete (i.e details of all the stores are written in the CSV file) corresponding entry in reports table is updated
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
        # Initialized the metrics
        metrics = {
            "store_id" : store_id,
            "uptime_last_hour" : 0,
            "uptime_last_day" : 0,
            "uptime_last_week" : 0,
            "downtime_last_hour" : 0,
            "downtime_last_day" : 0,
            "downtime_last_week" : 0
        }

        # Intialized the thresholds and dictionaries which are used store the duration between a threshold and first report generated after that
        last_hour_threshold = CURRENT_TIME - timedelta(hours=1)
        last_day_threshold = CURRENT_TIME - timedelta(days=1)
        last_week_threshold = CURRENT_TIME - timedelta(weeks=1)
        first_report_day = {
            "duration" : 0,
            "status" : ""
        }
        first_report_hour = {
            "duration" : 0,
            "status" : ""
        }
        first_report_week = {
            "duration" : 0,
            "status" : ""
        }

        # A list of status of store logged in last week is returned simultaneously marking if the activity is in last hour and last day
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

        # Timezone corresponding to storeID is fetched and if found none default timezone is assigned
        cursor.execute("SELECT timezone_str FROM timezone WHERE store_id = %s", (store_id,))
        timezone = cursor.fetchone()
        if timezone:
            timezone = timezone[0]
        else:
            timezone = DEFAULT_TIMEZONE
        
        # Business hours in local time for the store round the week is fetched
        cursor.execute("SELECT * FROM menu_hours WHERE store_id = %s", (store_id,))
        business_hours = cursor.fetchall()

        # current time and all the thresholds are converted into local time
        local_time = get_local_time(CURRENT_TIME, timezone)
        local_last_hour_threshold = get_local_time(last_hour_threshold, timezone)
        local_last_day_threshold = get_local_time(last_day_threshold, timezone)
        local_last_week_threshold = get_local_time(last_week_threshold, timezone)

        for index, entry in enumerate(store_reports):
            #Logic
            #If the report is genertated at 5pm (ACTIVE), 6pm (INACTIVE), 7pm (INACTIVE)
            #Menu hours are from 4pm to 9 pm
            # 4PM - 5PM ---> ACTIVE
            # 5PM - 6PM ---> ACTIVE
            # 6PM - 7PM ---> INACTIVE
            # 7PM - 9PM ---> INACTIVE
            # Using this logic uptime and downtime are calculated

            # Local day and time when status of store is noted is fetched
            report_time = get_local_time(store_reports[index][2], timezone)
            report_day = get_local_day(store_reports[index][2], timezone)

            # Based on the report day local start and end time of the store is fetched 
            local_start_time, local_end_time = get_local_hours(business_hours, report_day)

            # report_frameend is noted (i.e till what time is status of this report valid)
            # If the report is latest then it is valid till the current time
            # If not report generated after that is taken
            # If both of them are genertated on the same day then frameend is the report time of next generated status report
            # If both are of different days then frameend will be business hours of that day
            if (index > 0):
                last_report_day = get_local_day(store_reports[index-1][2], timezone)
                if (last_report_day == report_day):
                    report_frame_end = get_local_time(store_reports[index-1][2], timezone)
                else:
                    report_frame_end = local_end_time
            else:
                report_frame_end = local_time
            
            # the overlap duration between time frame and business hours is calculated
            duration = get_overlap_duration(local_start_time, local_end_time, report_time, report_frame_end)

            if store_reports[index][3] == True:
                # if the report is in last hour suration is added and duration between threshold and the report is noted (within that day business hours)
                start_duration = get_overlap_duration(local_start_time, local_end_time, local_last_hour_threshold, report_time)
                first_report_hour["duration"] = start_duration
                first_report_hour["status"] = store_reports[index][1]
                if store_reports[index][1] == ACTIVE:
                    metrics["uptime_last_hour"] += duration
                elif store_reports[index][1] == INACTIVE:
                    metrics["downtime_last_hour"] += duration

            if store_reports[index][4] == True:
                # if the report is in last day suration is added and duration between threshold and the report is noted (within that day business hours)
                start_duration = get_overlap_duration(local_start_time, local_end_time, local_last_day_threshold, report_time)
                first_report_day["duration"] = start_duration
                first_report_day["status"] = store_reports[index][1]
                if store_reports[index][1] == ACTIVE:
                    metrics["uptime_last_day"] += duration
                elif store_reports[index][1] == INACTIVE:
                    metrics["downtime_last_day"] += duration   
            
            # The duration between threshold and report time is noted (within that day business hours)
            start_duration = get_overlap_duration(local_start_time, local_end_time, local_last_week_threshold, report_time)
            first_report_week["duration"] = start_duration
            first_report_week["status"] = store_reports[index][1]
            if store_reports[index][1] == ACTIVE:
                metrics["uptime_last_week"] += duration
            elif store_reports[index][1] == INACTIVE:
                metrics["downtime_last_week"] += duration

        #Noted start durations are added
        if first_report_hour["status"] == ACTIVE:
            metrics["uptime_last_hour"] += first_report_hour["duration"]
        elif first_report_hour["status"] == INACTIVE:
            metrics["downtime_last_hour"] += first_report_hour["duration"]
        if first_report_day["status"] == ACTIVE:
            metrics["uptime_last_day"] += first_report_day["duration"]
        elif first_report_day["status"] == INACTIVE:
            metrics["downtime_last_day"] += first_report_day["duration"]
        if first_report_week["status"] == ACTIVE:
            metrics["uptime_last_week"] += first_report_week["duration"]
        elif first_report_week["status"] == INACTIVE:
            metrics["downtime_last_week"] += first_report_week["duration"]

        # If no report is generated in last hour latest report of that day is used to fill the gap
        if metrics["uptime_last_hour"] == 0 and metrics["downtime_last_hour"] == 0 and len(store_reports)>0:
            latest_report_datetime = get_local_datetime(store_reports[0][2], timezone)
            local_now_datetime = get_local_datetime(CURRENT_TIME, timezone)
            if are_same_day(latest_report_datetime, local_now_datetime):
                report_day = get_local_day(CURRENT_TIME, timezone)
                start, end = get_local_hours(business_hours, report_day)
                duration = get_overlap_duration(start, end, local_last_hour_threshold, local_time)
                if store_reports[0][1] == ACTIVE:
                    metrics["uptime_last_hour"] = duration
                elif store_reports[0][1] == INACTIVE:
                    metrics["downtime_last_hour"] = duration
        
        # Metrics are rounded off to two decimal places
        metrics["uptime_last_hour"] = round(metrics["uptime_last_hour"], 2)
        metrics["downtime_last_hour"] = round(metrics["downtime_last_hour"], 2)
        metrics["uptime_last_day"] = round(float(metrics["uptime_last_day"])/60, 2)
        metrics["downtime_last_day"] = round(float(metrics["downtime_last_day"])/60, 2)
        metrics["uptime_last_week"] = round(float(metrics["uptime_last_week"])/60, 2)
        metrics["downtime_last_week"] = round(float(metrics["downtime_last_week"])/60, 2)

        return metrics
    
    except psycopg2.Error as e:
        print(f"Error: {e}")
        return metrics