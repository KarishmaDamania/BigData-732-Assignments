import sys

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

from pyspark.sql import SparkSession, types

spark = SparkSession.builder.appName('weather train').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4'  # make sure we have Spark 2.4+

from pyspark.ml.feature import VectorAssembler, SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def main(input_path, output_filename):
    data = spark.read.csv(input_path, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    # data.show(10)

    sqlTrans = SQLTransformer(statement = """ SELECT today.latitude AS latitude,
           today.longitude AS longitude,
           today.elevation AS elevation,
           dayofyear(today.date) AS day_of_year,
           today.tmax AS tmax,
           yesterday.tmax AS yesterday_tmax
        FROM        __THIS__ as today
        INNER JOIN  __THIS__ as yesterday
        ON          date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station""")

    assemble_features = VectorAssembler(
        inputCols=['latitude', 'longitude', 'elevation', 'day_of_year', 'yesterday_tmax'],
        outputCol='features',
        handleInvalid='skip'
    )

    regressor = GBTRegressor(
        featuresCol='features', labelCol='tmax', maxIter = 100, maxDepth=5
    )

    pipeline = Pipeline(stages = [sqlTrans, assemble_features, regressor])
    model = pipeline.fit(train)
    predictions = model.transform(validation)
    # predictions.show()

    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='tmax',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print("r2 = ", r2)

    rmse_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='tmax',
        metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print("rmse = ", rmse)

    model.write().overwrite().save(output_filename)


if __name__ == '__main__':
    input_path = sys.argv[1]
    output_filename = sys.argv[2]
    main(input_path, output_filename)