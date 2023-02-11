from datetime import datetime as dt
from datetime import timedelta as td
import math
from dateutil import tz


local_time = dt.strptime('2022-06-11 00:29:49', '%Y-%m-%d %H:%M:%S')
local_time_adjusted = local_time - td(minutes=30)
utc_time = dt.strptime('2022-06-10 11:59:49', '%Y-%m-%d %H:%M:%S')

print(local_time)
print(local_time_adjusted)

print(utc_time)

adjust = 3