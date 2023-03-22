import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Column
from pyspark.sql.functions import col


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

    load_data = spark.read.csv(inputs, schema=observation_schema)
    load_data.createOrReplaceTempView("weather")

    qual_data = spark.sql("SELECT * FROM weather WHERE QFLAG IS NULL")
    qual_data.registerTempTable("correct_data")


    max_data = spark.sql("SELECT * FROM CORRECT_DATA WHERE OBSERVATION == 'TMAX' ")
    max_data.registerTempTable("max_tmp_obs")


    min_data = spark.sql("SELECT * FROM CORRECT_DATA WHERE OBSERVATION == 'TMIN' ")
    min_data.registerTempTable("min_tmp_obs")


    joined_table = spark.sql("SELECT max_tmp_obs.station, max_tmp_obs.date, max_tmp_obs.value AS TMAX, min_tmp_obs.value AS TMIN \
                          FROM max_tmp_obs INNER JOIN min_tmp_obs \
                          ON max_tmp_obs.date=min_tmp_obs.date \
                          AND max_tmp_obs.station=min_tmp_obs.station")
    joined_table.registerTempTable("joined_table")


    range_table = spark.sql("SELECT DATE, STATION, ROUND((TMAX/10 - TMIN/10),2) AS RANGE FROM JOINED_TABLE ")
    range_table.registerTempTable("table_w_range")


    group_by_range = spark.sql("SELECT DATE, MAX(RANGE) AS MAXRANGE \
                            FROM TABLE_W_RANGE \
                            GROUP BY DATE")
    group_by_range.registerTempTable("group_by_range")


    final_data = spark.sql("SELECT TABLE_W_RANGE.station, TABLE_W_RANGE.date, TABLE_W_RANGE.range \
                          FROM TABLE_W_RANGE INNER JOIN GROUP_BY_RANGE \
                          ON TABLE_W_RANGE.date=GROUP_BY_RANGE.date \
                          AND TABLE_W_RANGE.RANGE=GROUP_BY_RANGE.MAXRANGE \
                          ORDER BY TABLE_W_RANGE.date, TABLE_W_RANGE.station")
    final_data.registerTempTable("final_table")


if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather SQL').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)
