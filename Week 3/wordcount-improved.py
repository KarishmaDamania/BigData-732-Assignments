from pyspark import SparkConf, SparkContext
import sys
import re, string



def words_once(line):    
    regex = re.split(r'[%s\s]+' % re.escape(string.punctuation), line) # Regular expression to remove spaces and punctions 
    for word in regex:
        after_lower = word.lower() # Convert all words to lowercase
        yield (after_lower, 1)

def add(x, y):
    return x + y

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

def main(inputs, output):

    text = sc.textFile(inputs)
    text.repartition(10)
    words = text.flatMap(words_once)
    filtered_words = words.filter(lambda x: x[0] != " ")
    wordcount = filtered_words.reduceByKey(add)

    outdata = wordcount.sortBy(get_key).map(output_format)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)