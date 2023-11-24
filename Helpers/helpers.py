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