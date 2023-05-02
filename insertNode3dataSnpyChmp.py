from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from time import sleep
from datetime import datetime

import psutil
import numpy as np

ip = "10.10.1.4"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
print(zone)

itime = 1;
while itime <= 1152000:
    measurements_ = ["cpu_01","cpu_02","cpu_03","cpu_04","cpu_05","cpu_06","cpu_07","cpu_08","cpu_09","cpu_10","cpu_11","cpu_12","cpu_13","cpu_14","cpu_15","cpu_16"]
    data_types_ = [TSDataType.FLOAT for _ in range(len(measurements))]
    cpuPercent = psutil.cpu_percent(interval=0.001, percpu=True)
    session.insert_record("root.snpyChmp_node_03.percent", itime, measurements_, data_types_, cpuPercent)
    cpuFreq = psutil.cpu_freq(percpu=True)
    pcf = [freq[0] for freq in cpuFreq]
    session.insert_record("root.snpyChmp_node_03.freq", itime, measurements_, data_types_, pcf)
    itime += 1

# execute sql query statement
itime = itime-10
with session.execute_query_statement(
    "select * from root.snpyChmp_node_03.freq"
) as session_data_set:
    session_data_set.set_fetch_size(8192)
    #while session_data_set.has_next():
    while itime<=1152000:
        print(session_data_set.next())
        itime += 1
session.close()
