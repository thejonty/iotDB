import numpy as np

from iotdb.Session import Session
from iotdb.template.InternalNode import InternalNode
from iotdb.template.MeasurementNode import MeasurementNode
from iotdb.template.Template import Template
from iotdb.utils.BitMap import BitMap
from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
from iotdb.utils.Tablet import Tablet
from iotdb.utils.NumpyTablet import NumpyTablet

# creating session connection.
ip = "127.0.0.1"
port_ = "6667"
username_ = "root"
password_ = "root"
# session = Session(ip, port_, username_, password_, fetch_size=1024, zone_id="UTC+8", enable_redirection=True)
session = Session.init_from_node_urls(
    node_urls=["10.10.1.2:6667", "10.10.1.3:6667", "10.10.1.4:6667"],
    user="root",
    password="root",
    fetch_size=1024,
    zone_id="UTC+8",
    enable_redirection=True,
)
session.open(False)

# create and delete databases
session.set_storage_group("root.sg_test_01")
session.set_storage_group("root.sg_test_02")
session.set_storage_group("root.sg_test_03")
session.set_storage_group("root.sg_test_04")
session.delete_storage_group("root.sg_test_02")
session.delete_storage_groups(["root.sg_test_03", "root.sg_test_04"])

# setting time series.
session.create_time_series(
    "root.sg_test_01.d_01.s_01", TSDataType.BOOLEAN, TSEncoding.PLAIN, Compressor.SNAPPY
)
session.create_time_series(
    "root.sg_test_01.d_01.s_02", TSDataType.INT32, TSEncoding.PLAIN, Compressor.SNAPPY
)
session.create_time_series(
    "root.sg_test_01.d_01.s_03", TSDataType.INT64, TSEncoding.PLAIN, Compressor.SNAPPY
)
session.create_time_series(
    "root.sg_test_01.d_02.s_01",
    TSDataType.BOOLEAN,
    TSEncoding.PLAIN,
    Compressor.SNAPPY,
    None,
    {"tag1": "v1"},
    {"description": "v1"},
    "temperature",
)
