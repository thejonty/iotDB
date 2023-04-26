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
#print(zone)

# setting multiple time series once.
ts_path_lst_ = [
    "root.snpyChmp_node_03.percent.cpu_01",
    "root.snpyChmp_node_03.percent.cpu_02",
    "root.snpyChmp_node_03.percent.cpu_03",
    "root.snpyChmp_node_03.percent.cpu_04",
    "root.snpyChmp_node_03.percent.cpu_05",
    "root.snpyChmp_node_03.percent.cpu_06",
    "root.snpyChmp_node_03.percent.cpu_07",
    "root.snpyChmp_node_03.percent.cpu_08",
    "root.snpyChmp_node_03.percent.cpu_09",
    "root.snpyChmp_node_03.percent.cpu_10",
    "root.snpyChmp_node_03.percent.cpu_11",
    "root.snpyChmp_node_03.percent.cpu_12",
    "root.snpyChmp_node_03.percent.cpu_13",
    "root.snpyChmp_node_03.percent.cpu_14",
    "root.snpyChmp_node_03.percent.cpu_15",
    "root.snpyChmp_node_03.percent.cpu_16"
]
data_type_lst_ = [TSDataType.FLOAT for _ in range(len(ts_path_lst_))]
encoding_lst_ = [TSEncoding.CHIMP for _ in range(len(data_type_lst_))]
compressor_lst_ = [Compressor.SNAPPY for _ in range(len(data_type_lst_))]
session.create_multi_time_series(
    ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
)

ts_path_lst_ = [
    "root.snpyChmp_node_03.freq.cpu_01",
    "root.snpyChmp_node_03.freq.cpu_02",
    "root.snpyChmp_node_03.freq.cpu_03",
    "root.snpyChmp_node_03.freq.cpu_04",
    "root.snpyChmp_node_03.freq.cpu_05",
    "root.snpyChmp_node_03.freq.cpu_06",
    "root.snpyChmp_node_03.freq.cpu_07",
    "root.snpyChmp_node_03.freq.cpu_08",
    "root.snpyChmp_node_03.freq.cpu_09",
    "root.snpyChmp_node_03.freq.cpu_10",
    "root.snpyChmp_node_03.freq.cpu_11",
    "root.snpyChmp_node_03.freq.cpu_12",
    "root.snpyChmp_node_03.freq.cpu_13",
    "root.snpyChmp_node_03.freq.cpu_14",
    "root.snpyChmp_node_03.freq.cpu_15",
    "root.snpyChmp_node_03.freq.cpu_16"
]

compressor_lst_ = [Compressor.LZ4 for _ in range(len(data_type_lst_))]
encoding_lst_ = [TSEncoding.GORILLA for _ in range(len(data_type_lst_))]

session.create_multi_time_series(
    ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
)

sleep(1)
# execute sql query statement
with session.execute_query_statement(
    "select * from root.snpyChmp_node_03.percent"
) as session_data_set:
    session_data_set.set_fetch_size(8192)
    while session_data_set.has_next():
        print(session_data_set.next())
session.close()
