from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import json # make sure we have Python 3.5+

# add more functions as necessary


def convert_json(object):
    record = json.loads(object)
    return record

def make_key_value_pair(record):
    subredit_name = record["subreddit"]
    score = record["score"]
    count = 1
    return (subredit_name, (score, count))

def add(x,y):
    total_score = x[0] + y[0]
    total_count = x[1] + y[1]
    return (total_score, total_count)

def get_key(kv):
    return kv[0]

def get_averages(record):
    key, value = record
    score, count = value
    average = score/count
    json_record = json.dumps([key, average])
    return json_record



def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)  
    json_objects = text.map(convert_json)
    key_value_pairs = json_objects.map(make_key_value_pair)
    total_scores_by_key = key_value_pairs.sortBy(get_key).reduceByKey(add)
    reddit_averages = total_scores_by_key.map(get_averages)

    reddit_averages.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
