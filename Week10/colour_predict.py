import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.4' # hdmake sure we have Spark 2.4+

from pyspark.ml.pipeline import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()
    
    # TODO: create a pipeline to predict RGB colours -> word
    
    rgb_assembler = VectorAssembler(
        inputCols=['R', 'G', 'B'],
        outputCol='features'
    )

    word_indexer = StringIndexer(inputCol="word", outputCol="indexed_targets",
    stringOrderType="frequencyDesc", handleInvalid="error")

    classifier = MultilayerPerceptronClassifier(layers=[3, 30, 11], 
    featuresCol='features', labelCol='indexed_targets')


    rgb_pipeline = Pipeline(stages=[rgb_assembler, word_indexer, classifier])
    rgb_model = rgb_pipeline.fit(train)
    predictions = rgb_model.transform(validation)
    # predictions.show()
    
    # TODO: create an evaluator and score the validation data
    
    evaluator =  MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='indexed_targets')
    score = evaluator.evaluate(predictions)
    plot_predictions(rgb_model, 'RGB', labelCol='word')
    print('Validation score for RGB model: %g' % (score, ))
    
    # TODO: create a pipeline RGB colours -> LAB colours -> word; train and evaluate.
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=["word"])
    sqlTrans = SQLTransformer(statement = rgb_to_lab_query)

    LAB_assemble_features = VectorAssembler(
        inputCols=['labL', 'labA', 'labB'],
        outputCol='features'
    )

    LAB_pipeline = Pipeline(stages = [sqlTrans, LAB_assemble_features, word_indexer, classifier])
    LAB_model = LAB_pipeline.fit(train)
    LAB_predictions = LAB_model.transform(validation)
    # LAB_predictions.show()

    LAB_score = evaluator.evaluate(LAB_predictions)
    plot_predictions(LAB_model, 'LAB', labelCol='word')
    print('Validation score for LAB model: %g' % (LAB_score, ))
    


    
if __name__ == '__main__':
    inputs = sys.argv[1]
    main(inputs)
