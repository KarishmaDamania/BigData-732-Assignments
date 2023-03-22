import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Column
from pyspark.sql.functions import col

# add more functions as necessary

def main(inputs, output):
    observation_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.StringType()),
    types.StructField('observation', types.StringType()),
    types.StructField('value', types.IntegerType()),
    types.StructField('mflag', types.StringType()),
    types.StructField('qflag', types.StringType()),
    types.StructField('sflag', types.StringType()),
    types.StructField('obstime', types.StringType()),
    ])

    weather = spark.read.format("s3selectCSV").schema(observation_schema).options(compression='gzip').load(inputs)
    correct_data = weather.filter(weather.qflag.isNull())
    canadian_data = correct_data.filter(correct_data.station.startswith('CA'))
    max_tmp_obs = canadian_data.filter(canadian_data.observation.startswith("TMAX"))
    with_temp = max_tmp_obs.withColumn("tmax", max_tmp_obs.value / 10)
    final_etl_data = with_temp.select("station", "date", "tmax")
    final_etl_data.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather_ETL_S3_Select').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)