from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from time import sleep
from datetime import datetime

import psutil
import numpy as np

ip = "10.10.1.2"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
print(zone)
#session.set_storage_group("root.sg_cpu_02")
#session.create_time_series("root.sg_cpu_02.core1.t_01", TSDataType.FLOAT, TSEncoding.PLAIN, Compressor.SNAPPY)
#session.insert_record("root.sg_cpu_02.core1.t_02", datetime.now(), ["test"], [TSDataType.FLOAT], [90.99])

#session.delete_storage_group("root.sg_cpu_02")
itime = 1;
while itime <= 1:#152000:
    data_types_ = [
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT,
            TSDataType.FLOAT]
    cpuPercent = psutil.cpu_percent(interval=0.1, percpu=True)
    measurements_ = ["cpu01","cpu02","cpu03","cpu04","cpu05","cpu06","cpu07","cpu08","cpu09","cpu10","cpu11","cpu12","cpu13","cpu14","cpu15","cpu16"];
    session.insert_record("root.snpy_node_03.percent", itime, measurements_, data_types_, cpuPercent)
    
    cpuFreq = psutil.cpu_freq(percpu=True)
    pcf = [freq[0] for freq in cpuFreq]
    session.insert_record("root.snpy_node03.freq", itime, measurements_, data_types_, pcf)
    itime += 1

# execute sql query statement
with session.execute_query_statement(
    "select cpu01 from root.snpy_node03.freq"
) as session_data_set:
    session_data_set.set_fetch_size(8192)
    while session_data_set.has_next():
        print(session_data_set.next())
session.close()
