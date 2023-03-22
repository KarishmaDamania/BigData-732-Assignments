
import sys
import re
from pyspark.sql import SparkSession, functions, types
from pyspark import SparkConf
import datetime
import uuid



def get_log_rows(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    match = re.search(line_re, line)
    if match:
        m = re.match(line_re, line)
        uid = str(uuid.uuid1())
        host = m.group(1)
        date=datetime.datetime.strptime(m.group(2), '%d/%b/%Y:%H:%M:%S')
        path=m.group(3)
        bytes = int(m.group(4))
        return uid, host, date, path, bytes
    return None

#Schema for nasalogs Table
logs_schmea = types.StructType([
    types.StructField('uid',types.StringType(),False),
    types.StructField('host', types.StringType(), False),
    types.StructField('datetime', types.TimestampType(), False),
    types.StructField('path', types.StringType(), False),
    types.StructField('bytes', types.IntegerType(), False),
])

def main(input_dir,user_id,table_name):

    #Load and cache logs
    logs = sc.textFile(input_dir).cache()
    #Make sure data is not None & repartition the data 
    log_rows = logs.map(get_log_rows).filter(lambda x: x is not None)
    log_rows = log_rows.repartition(128)
    df=spark.createDataFrame(log_rows,logs_schmea)
    df.write.format("org.apache.spark.sql.cassandra").options( table = table_name, keyspace = user_id).save(mode="append")

if __name__ == '__main__':
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Load Logs Spark').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    input_dir = sys.argv[1]
    user_id = sys.argv[2]   
    table_name = sys.argv[3]
    sc = spark.sparkContext
    main(input_dir,user_id,table_name)