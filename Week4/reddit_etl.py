from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import json # make sure we have Python 3.5+

# add more functions as necessary


def convert_to_json(object):
    record = json.loads(object)
    req_rec = {"author": record["author"], 
                "score": record["score"], 
                "subreddit": record["subreddit"]}
    return req_rec

def remove_e(object):
    if "e" in object["subreddit"]:
        return object

def check_negative(object):
    if object is not None:
        if object["score"] <= 0: 
            return object

def check_positive(object):
    if object is not None:
        if object["score"] > 0: 
            return object





def main(inputs, output):
    # main logic starts here
    text = sc.textFile(inputs)
    json_objects = text.map(convert_to_json)
    filtered_records = json_objects.map(remove_e).cache()
    filtered_records.filter(check_positive).saveAsTextFile(output + '/positive')
    filtered_records.filter(check_negative).saveAsTextFile(output + '/negative')
    

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
