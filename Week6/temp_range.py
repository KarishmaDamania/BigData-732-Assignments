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

    weather = spark.read.csv(inputs, schema=observation_schema)

    #Filtering to get needed data 
    #Qflag = NULL
    #Create two tables - one w TMAXs & one w TMINS
    correct_data = weather.filter(weather.qflag.isNull())
    max_tmp_obs = correct_data.filter(correct_data.observation.startswith("TMAX")).withColumn("tmax", (correct_data.value / 10))
    min_tmp_obs = correct_data.filter(correct_data.observation.startswith("TMIN")).withColumn("tmin", (correct_data.value / 10))

    # Join to get table w Range
    full_table_w_range = max_tmp_obs.join(min_tmp_obs, ['date', 'station'], "inner").withColumn("range", functions.round((max_tmp_obs['tmax'] - min_tmp_obs['tmin']), 2)).cache()

    #Group by date & station on max range
    group_by_range = full_table_w_range.groupBy('date').max('range')

    # Broadcast Join group_by_range (count = 31) w full_table_w_range (count = 3638)
          #Join on range and date to get the station
    # Select needed columns
    # OrderBy Date & Station 
    final_data = full_table_w_range.join(functions.broadcast(group_by_range), (group_by_range.date == full_table_w_range.date) &  (group_by_range['max(range)'] == full_table_w_range['range'])).drop(full_table_w_range.date).select('date', 'station', 'range').orderBy('date', 'station')
    final_data.write.csv(output, mode='overwrite')



if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    spark = SparkSession.builder.appName('Weather Dataframe').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs, output)