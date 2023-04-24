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
ilines = 10000
with session.execute_query_statement(
    "select * from root.snpy_node_03.percent"
) as session_data_set:
    session_data_set.set_fetch_size(8192)
    while ilines < 10020: #session_data_set.has_next():
        print(session_data_set.next())
        ilines += 1
session.close()
