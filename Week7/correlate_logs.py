import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types, Column, Row
from pyspark.sql.functions import col

import re

def hostname_bytes_pair(data):
    #Ensure Data is not null and convert bytes_trans to int
    while data!=None:
        host_name, date_time, req_path, bytes_trans = data.groups()

        #Return as Row objects
        return Row(host_name, int(bytes_trans))


def main(inputs):

    #Load & Clean Logs as RDDs
    logs = sc.textFile(inputs)
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    cleaned_logs = logs.map(lambda x: re.search(line_re, x))
    log_rows = cleaned_logs.map(hostname_bytes_pair).filter(lambda x: x!=None)

    #Define Schema
    observation_schema = types.StructType([
    types.StructField('host_name', types.StringType()),
    types.StructField('bytes_trans', types.LongType())
    ])

    #Create the Dataframe from RDD
    logs_df = spark.createDataFrame(log_rows, schema = observation_schema)
    grouped_by_host = logs_df.groupBy('host_name').agg(functions.count('host_name').alias('xi'), functions.sum('bytes_trans').alias('yi'))

    #Define the rest of the sums
    correlation_df = grouped_by_host.withColumn('n', functions.lit(1))\
                                    .withColumn('xi_square', (grouped_by_host['xi']**2))\
                                    .withColumn('yi_square', (grouped_by_host['yi']**2))\
                                    .withColumn('xi_yi', (grouped_by_host['xi'] * grouped_by_host['yi']))

    #Sum of the columns and .collect() to convert to a Row object
    #[Row(sum(xi)=1972, sum(yi)=36133736, sum(n)=232, sum(xi_square)=32560.0, sum(yi_square)=25731257461526.0, sum(xi_yi)=662179733)]
    #Convert Row to dictionary for cleaner code
    sums = correlation_df.groupBy().sum().collect()
    sum_of_values = sums[0].asDict()

    #Correlational Coefficient Formula (r & r^2)
    corr_coef = ( (sum_of_values['sum(n)'] * sum_of_values['sum(xi_yi)']) - (sum_of_values['sum(xi)'] * sum_of_values['sum(yi)']) ) \
         / ( ( ( (sum_of_values['sum(n)'] * sum_of_values['sum(xi_square)']) - (sum_of_values['sum(xi)']**2) ) **0.5 ) * ( ((sum_of_values['sum(n)'] * sum_of_values['sum(yi_square)']) - (sum_of_values['sum(yi)']**2))**0.5 ) )
    corr_coef_square = corr_coef**2

    #Print Outputß
    print("r = ", corr_coef)
    print("r^2 = ", corr_coef_square)
    

if __name__ == '__main__':
    inputs = sys.argv[1]
    spark = SparkSession.builder.appName('Correlation').getOrCreate()
    assert spark.version >= '3.0' # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(inputs)

