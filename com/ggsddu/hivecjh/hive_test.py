from pyhive import hive
from pyhive.exc import OperationalError
import time

conn = hive.connect(host='192.168.7.150', port=10000, username='hdfs', database='default')
cursor = conn.cursor()
start = int(time.time())
try:
    # cursor.execute('CREATE TABLE parquet_probe_data(mac STRING,place_code BIGINT,time_on BIGINT,time_off BIGINT,count INT) STORED AS PARQUET')
    # cursor.execute('show tables')
    # cursor.execute(
    #     "LOAD DATA INPATH '/weihai/compress/probe_type/20201201/part-00000-dee7414a-1264-46ca-babe-f642b175e36c-c000.snappy.parquet' OVERWRITE INTO TABLE parquet_probe_data")

    cursor.execute('select * from parquet_probe_data limit 200')
    for result in cursor.fetchall():
        print(result)
except OperationalError as err:
    print(err)
    raise
end = int(time.time())
print(end - start)
# LOAD DATA INPATH '/a.txt' OVERWRITE INTO TABLE behavior_table;