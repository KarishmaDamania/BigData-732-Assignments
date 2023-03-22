from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
output = sys.argv[2]


conf = SparkConf().setAppName('WikipediaPopular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+





def records(lines):
    date, lang, title, count, byte = lines.split()
    yield (date, lang, title, int(count), byte)

def remove_some(record):
    if (record[2].startswith("Special:") | record[2].startswith("Main_Page") == False):
        return record

def get_key(kv):
    return kv[0]

def get_max(x,y):
    return y if y>x else x 


def make_key_value_pair(record):
    date, lang, title, requests, size = record
    return date, (requests, title)

def tab_separated(kv):
    return "%s\t%s" % (kv[0], (kv[1][0], kv[1][1]))




text = sc.textFile(inputs)
all_records = text.flatMap(records)
only_english_records = all_records.filter(lambda x: x[1] == "en")
required_records = only_english_records.filter(remove_some).map(make_key_value_pair)
max_records = required_records.sortBy(get_key).reduceByKey(get_max)
max_records.map(tab_separated).saveAsTextFile(output)

