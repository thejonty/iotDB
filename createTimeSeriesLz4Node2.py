from iotdb.Session import Session
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from time import sleep
from datetime import datetime

import psutil
import numpy as np

ip = "10.10.1.3"
port_ = "6667"
username_ = "root"
password_ = "root"
session = Session(ip, port_, username_, password_)
session.open(False)
zone = session.get_time_zone()
#print(zone)

# setting multiple time series once.

ts_path_lst_ = [
    "root.lz4_node_02.percent.cpu_01",
    "root.lz4_node_02.percent.cpu_02",
    "root.lz4_node_02.percent.cpu_03",
    "root.lz4_node_02.percent.cpu_04",
    "root.lz4_node_02.percent.cpu_05",
    "root.lz4_node_02.percent.cpu_06",
    "root.lz4_node_02.percent.cpu_07",
    "root.lz4_node_02.percent.cpu_08",
    "root.lz4_node_02.percent.cpu_09",
    "root.lz4_node_02.percent.cpu_10",
    "root.lz4_node_02.percent.cpu_11",
    "root.lz4_node_02.percent.cpu_12",
    "root.lz4_node_02.percent.cpu_13",
    "root.lz4_node_02.percent.cpu_14",
    "root.lz4_node_02.percent.cpu_15",
    "root.lz4_node_02.percent.cpu_16"
]
data_type_lst_ = [TSDataType.FLOAT for _ in range(len(ts_path_lst_))]
compressor_lst_ = [Compressor.LZ4 for _ in range(len(data_type_lst_))]

encoding_lst_ = [TSEncoding.CHIMP for _ in range(len(data_type_lst_))]
#CHIMP for percent
session.create_multi_time_series(
    ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
)

encoding_lst_ = [TSEncoding.PLAIN for _ in range(len(data_type_lst_))]
#PLAIN for percent
ts_path_lst_ = [
    "root.lz4_node_02.freq.cpu_01",
    "root.lz4_node_02.freq.cpu_02",
    "root.lz4_node_02.freq.cpu_03",
    "root.lz4_node_02.freq.cpu_04",
    "root.lz4_node_02.freq.cpu_05",
    "root.lz4_node_02.freq.cpu_06",
    "root.lz4_node_02.freq.cpu_07",
    "root.lz4_node_02.freq.cpu_08",
    "root.lz4_node_02.freq.cpu_09",
    "root.lz4_node_02.freq.cpu_10",
    "root.lz4_node_02.freq.cpu_11",
    "root.lz4_node_02.freq.cpu_12",
    "root.lz4_node_02.freq.cpu_13",
    "root.lz4_node_02.freq.cpu_14",
    "root.lz4_node_02.freq.cpu_15",
    "root.lz4_node_02.freq.cpu_16"
]

session.create_multi_time_series(
    ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
)

sleep(1)
# execute sql query statement
with session.execute_query_statement(
    "select cpu01 from root.sg_node_02.percent"
) as session_data_set:
    session_data_set.set_fetch_size(8192)
    while session_data_set.has_next():
        print(session_data_set.next())
session.close()
