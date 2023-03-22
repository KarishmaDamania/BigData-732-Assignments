from pyspark import SparkConf, SparkContext
import sys
assert sys.version_info >= (3, 5)
import random as rd # make sure we have Python 3.5+
import time

# add more functions as necessary

def add(x,y):
    return x+y

def iterator(itr):
    iterations = 0
    for i in range(int(itr)):
        sum = 0.0
        rd.seed()

        while sum <1:
            sum+= rd.random()
            iterations+=1
    return iterations


def main(inputs):
    samples = int(inputs)
    num_partitions = 4
    rdd = sc.parallelize([samples/num_partitions] * num_partitions, num_partitions)
    iterations = rdd.map(iterator)
    total_iterations = iterations.reduce(add)
    print("Sample Size: ", int(inputs))
    print("Eulers No:", total_iterations/samples)
    print("Number of Partitions:", num_partitions)


if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '3.0'  # make sure we have Spark 3.0+
    inputs = sys.argv[1]
    main(inputs)
