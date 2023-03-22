from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import json # make sure we have Python 3.5+

# add more functions as necessary

def make_key_value_pair(record):
    subredit_name = record["subreddit"]
    score = record["score"]
    count = 1
    return (subredit_name, (score, count))


def subreddit_comment_pairs(record):
    subreddit_name = record["subreddit"]
    return (subreddit_name, record)

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
    json_record = (key, average)
    return json_record

def get_relative_score(comment, value):
    relative_score = comment[1]["score"]/value[comment[0]]
    return (relative_score, comment[1]["author"])


def only_positive(record):
    key, average = record
    if (average > 0):
        return record

def main(inputs, output):
    # main logic starts here
    records_in_json = sc.textFile(inputs).map(json.loads).cache()
    key_value_pairs = records_in_json.map(make_key_value_pair)
    total_scores_by_key = key_value_pairs.sortBy(get_key).reduceByKey(add)
    reddit_averages = total_scores_by_key.map(get_averages)
    positive_averages_only = reddit_averages.filter(only_positive)

    averages_dict = dict(positive_averages_only.collect())
    comment_by_sub = records_in_json.map(subreddit_comment_pairs)
    broadcast_var = sc.broadcast(averages_dict)


    after_join = comment_by_sub.map(lambda x: get_relative_score(x, broadcast_var.value))

    final_sort = after_join.sortBy(lambda x: x[0], False)
    final_sort.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('Relative Score Bcast')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)