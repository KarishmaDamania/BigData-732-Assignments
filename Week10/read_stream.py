
import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types


def main(topic):
    messages = spark.readStream.format('kafka') \
        .option('kafka.bootstrap.servers', 'node1.local:9092,node2.local:9092') \
        .option('subscribe', topic).load()
    values = messages.select(messages['value'].cast('string'))

    xy_df = values.select(functions.split(values['value'], ' ').getItem(0).alias('x'), functions.split(values['value'], ' ').getItem(1).alias('y'))

    computed_values_df = xy_df.select(
        xy_df['x'], xy_df['y'],
        (xy_df['x'] * xy_df['y']).alias('x*y'),
        (xy_df['x'] ** 2).alias('x_square'),
        (xy_df['y'] ** 2).alias('y_square'),
        functions.lit(1).alias('n')
        )

    sum_df = computed_values_df.agg(
        functions.sum(computed_values_df['x']).alias('x'),
        functions.sum(computed_values_df['y']).alias('y'),
        functions.sum(computed_values_df['x*y']).alias('xy'),
        functions.sum(computed_values_df['x_square']).alias('x**2'),
        functions.sum(computed_values_df['y_square']).alias('y**2'),
        functions.sum(computed_values_df['n']).alias('n'),
    )


    slope_df = sum_df.withColumn('slope', (sum_df['xy'] - (1/sum_df['n'] * sum_df['x'] * sum_df['y'])) /
                                      (sum_df['x**2'] - (1/sum_df['n'] * (sum_df['x'] ** 2))))
    intercept_df = slope_df.withColumn('intercept', (slope_df['y'] / slope_df['n']) - (slope_df['slope'] * (slope_df['x'] / slope_df['n'])))

    results_df = intercept_df.select(intercept_df['slope'], intercept_df['intercept'])

    stream = results_df.writeStream.format('console') \
        .outputMode('update').start()
    stream.awaitTermination(100)

if __name__ == '__main__':
    topic = sys.argv[1]
    spark = SparkSession.builder.appName('streaming example').getOrCreate()
    assert spark.version >= '2.4'  # make sure we have Spark 2.4+
    spark.sparkContext.setLogLevel('WARN')
    sc = spark.sparkContext
    main(topic)