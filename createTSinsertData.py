def createCpuTimeSeries(ipAddr, seriesName, cpuMetric, compressorName, encoderName, nMeasurements):
    from iotdb.Session import Session
    from iotdb.utils.IoTDBConstants import TSDataType, TSEncoding, Compressor
    from time import sleep, time
    from datetime import datetime

    import psutil
    import numpy as np
    
    ip = ipAddr
    port_ = "6667"
    username_ = "root"
    password_ = "root"
    session = Session(ip, port_, username_, password_)
    session.open(False)
    zone = session.get_time_zone()
    #print(zone)
    
    # setting multiple time series once.
    ts_path_lst_ = [
        "root." + seriesName + "." + cpuMetric + ".cpu_01",
        "root." + seriesName + "." + cpuMetric + ".cpu_02",
        "root." + seriesName + "." + cpuMetric + ".cpu_03",
        "root." + seriesName + "." + cpuMetric + ".cpu_04",
        "root." + seriesName + "." + cpuMetric + ".cpu_05",
        "root." + seriesName + "." + cpuMetric + ".cpu_06",
        "root." + seriesName + "." + cpuMetric + ".cpu_07",
        "root." + seriesName + "." + cpuMetric + ".cpu_08",
        "root." + seriesName + "." + cpuMetric + ".cpu_09",
        "root." + seriesName + "." + cpuMetric + ".cpu_10",
        "root." + seriesName + "." + cpuMetric + ".cpu_11",
        "root." + seriesName + "." + cpuMetric + ".cpu_12",
        "root." + seriesName + "." + cpuMetric + ".cpu_13",
        "root." + seriesName + "." + cpuMetric + ".cpu_14",
        "root." + seriesName + "." + cpuMetric + ".cpu_15",
        "root." + seriesName + "." + cpuMetric + ".cpu_16"
    ]

    data_type_lst_ = [TSDataType.FLOAT for _ in range(len(ts_path_lst_))]
    
    if compressorName == "SNAPPY":
        compressorType = Compressor.SNAPPY
    elif compressorName == "LZ4":
       compressorType = Compressor.LZ4
    elif compressorName == "GZIP":
       compressorType = Compressor.GZIP
    elif compressorName == "UNCOMPRESSED":
       compressorType = Compressor.UNCOMPRESSED

    if encoderName == "CHIMP":
        encodingType=TSEncoding.CHIMP
    elif encoderName == "GORILLA":
        encodingType=TSEncoding.GORILLA
    elif encoderName == "PLAIN":
        encodingType=TSEncoding.PLAIN

    encoding_lst_ = [encodingType for _ in range(len(data_type_lst_))]
    compressor_lst_ = [compressorType for _ in range(len(data_type_lst_))]
    session.create_multi_time_series(
        ts_path_lst_, data_type_lst_, encoding_lst_, compressor_lst_
    )
    sleep(1)
    ts_path = "root." + seriesName + "." + cpuMetric + "comprTime"

    session.create_time_series(ts_path, TSDataType.FLOAT, encodingType, compressorType)

    measurements_ = ["cpu_01","cpu_02","cpu_03","cpu_04","cpu_05","cpu_06","cpu_07","cpu_08","cpu_09","cpu_10","cpu_11","cpu_12","cpu_13","cpu_14","cpu_15","cpu_16"]
    compressionTimeSum = 0
    itime = 0
    while itime <= nMeasurements:
        data_types_ = [TSDataType.FLOAT for _ in range(len(measurements_))]
        if cpuMetric == "percent":
            cpuPercent = psutil.cpu_percent(interval=0.001, percpu=True)
            startTime = time()
            session.insert_record("root." + seriesName + "." + cpuMetric, itime, measurements_, data_types_, cpuPercent)
            endTime = time()
        elif cpuMetric == "freq":
            cpuFreq = psutil.cpu_freq(percpu=True)
            pcf = [freq[0] for freq in cpuFreq]
            startTime = time()
            session.insert_record("root." + seriesName + "." + cpuMetric, itime, measurements_, data_types_, pcf)
            endTime = time()
        
        comprTime = endTime - startTime
        session.insert_record("root." + seriesName + "." + cpuMetric, itime, ["comprTime"], [TSDataType.FLOAT], [comprTime])
        itime += 1
        compressionTimeSum += comprTime
        #print(comprTime)
        #sleep(1e-6)
    print("Average compression time = ", compressionTimeSum/nMeasurements)
    session.close()

if __name__ == '__main__':
#    seriesName = "SnChTest"
    ipAddr = "10.10.1.4"
    cpuMetric = "freq"
    compressorNameList = ["UNCOMPRESSED"]
    encoderNameList = ["CHIMP", "PLAIN", "GORILLA"]
    nMeasurements = 1e6
    for compressorName in compressorNameList:
        for encoderName in encoderNameList:
            seriesName = compressorName + "_" + encoderName
            createCpuTimeSeries(ipAddr, seriesName, cpuMetric, compressorName, encoderName, nMeasurements)
            print("created and inserted data into TS: " + seriesName)
