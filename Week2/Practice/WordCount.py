from pyspark import SparkConf, SparkContext
import sys

inputs = sys.argv[1]
outputs = sys.argv[2]

conf = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=conf)
assert sys.version_info >= (3,5)
assert sc.version >= '2.3'

def words_once(line):
    for w in line:
        yield (w, 1)

def add(x,y):
    print("x =", x)
    print("y= ", y)
    return x+y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k,v = kv
    return "%s %i" % (k,v)


text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.reduceByKey(add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(outputs)