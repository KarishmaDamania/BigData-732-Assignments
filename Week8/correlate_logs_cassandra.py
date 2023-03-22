

import sys
import math
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def main(keyspace, table_name):
    logs_df = spark.read.format("org.apache.spark.sql.cassandra") \
        .options(table=table_name, keyspace=keyspace).load()

    grouped_by_host = logs_df.groupBy('host').agg(functions.count('host').alias('xi'), functions.sum('bytes').alias('yi'))

    #Define the rest of the sums
    correlation_df = grouped_by_host.withColumn('n', functions.lit(1))\
                                    .withColumn('xi_square', (grouped_by_host['xi']**2))\
                                    .withColumn('yi_square', (grouped_by_host['yi']**2))\
                                    .withColumn('xi_yi', (grouped_by_host['xi'] * grouped_by_host['yi']))

    #Sum of the columns and .collect() to convert to a Row object
    
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
    keyspace = sys.argv[1]
    table_name = sys.argv[2]
    cluster_seeds = ['node1.local', 'node2.local']
    spark = SparkSession.builder.appName('Spark Cassandra correlate logs') \
        .config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    assert spark.version >= '3.0'  # make sure we have Spark 3.0+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(keyspace, table_name)

# spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.1.0 --conf spark.sql.extensions=com.datastax.spark.connector.CassandraSparkExtensions correlate_logs_cassandra.py kpd3 nasalogs