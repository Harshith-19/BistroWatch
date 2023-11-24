from datetime import datetime
import pytz
from Helpers.constants import *

def get_local_time(timestamp_utc, timezone_str):
    try:
        datetime_utc = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S.%f UTC')
        timezone = pytz.timezone(timezone_str)
        datetime_local = datetime_utc.replace(tzinfo=pytz.UTC).astimezone(timezone)
        datetime_local = datetime_local.strftime('%H:%M:%S')
        return datetime_local
    except (ValueError, pytz.UnknownTimeZoneError):
        return None

def get_local_day(timestamp_utc, timezone_str):
    try:
        datetime_utc = datetime.strptime(timestamp_utc, '%Y-%m-%d %H:%M:%S.%f UTC')
        timezone = pytz.timezone(timezone_str)
        datetime_local = datetime_utc.replace(tzinfo=pytz.UTC).astimezone(timezone)
        day_local = datetime_local.weekday()
        return day_local
    except (ValueError, pytz.UnknownTimeZoneError):
        return None

def get_local_hours(business_hours, day):
    for i in range(len(business_hours)):
        if business_hours[i][1] == day:
            return business_hours[i][2], business_hours[i][3]
    return DEFAULT_START_LOCAL, DEFAULT_END_LOCAL

def get_overlap_duration(time_frame1_start, time_frame1_end, time_frame2_start, time_frame2_end):
    try :
        start1 = datetime.strptime(time_frame1_start, "%H:%M:%S")
        end1 = datetime.strptime(time_frame1_end, "%H:%M:%S")
        start2 = datetime.strptime(time_frame2_start, "%H:%M:%S")
        end2 = datetime.strptime(time_frame2_end, "%H:%M:%S")

        start_interval = max(start1, start2)
        end_interval = min(end1, end2)

        if start_interval < end_interval:
            duration = (end_interval - start_interval).total_seconds() / 60 
            return duration
        else:
            return 0
    except :
        return 0