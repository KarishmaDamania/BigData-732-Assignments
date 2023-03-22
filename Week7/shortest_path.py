from pyspark import SparkConf, SparkContext
import sys
import json
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def make_kv_pair(graph_edges):
    key, value_str = graph_edges.split(':')
    values = value_str.split(' ')
    values.remove('')
    return (key,values)

def find_new_path(element):
    new_paths=[]
    for node in element[1][1]:
        new_path=(node,[element[0],int(element[1][0][1])+1])
        new_paths.append(new_path)
    return new_paths

def get_shortest_path(a,b):
    if a[1]<b[1]:
        return a
    else:
        return b

def main(inputs, output, source, destination):
    graph_edges=sc.textFile(inputs+'/links-simple-sorted.txt')
    #RDD to hold the node and edges pairs
    graph_edges=graph_edges.map(make_kv_pair).cache()
    #Initialize known path as (source,['-'], '0')
    known_paths=sc.parallelize([(source,['-','0'])])

    for i in range(6):
        initial_list=known_paths.join(graph_edges)
        new_paths=initial_list.flatMap(find_new_path)
        known_paths=known_paths.union(new_paths)
        # Remove the repeating path and keep the shortest length path
        known_paths=known_paths.reduceByKey(get_shortest_path)

        known_paths.saveAsTextFile(output + '/iter-' + str(i))
        if known_paths.lookup(destination):
            break

# print("initial_list=",initial_list.collect())
# print("New_Paths=", new_paths.collect())
# print("Known_Paths=", known_paths.collect())
# print("Known_Paths after reducing =",known_paths.collect())

# initial_list= [('1', (['-', '0'], ['3', '5']))]
# New_Paths= [('3', ['1', 1]), ('5', ['1', 1])]
# Known_Paths= [('1', ['-', '0']), ('3', ['1', 1]), ('5', ['1', 1])]
# Known_Paths after reducing = [('3', ['1', 1]), ('1', ['-', '0']), ('5', ['1', 1])]

# initial_list= [('1', (['-', '0'], ['3', '5'])), ('3', (['1', 1], ['2', '5'])), ('5', (['1', 1], []))]
# New_Paths= [('3', ['1', 1]), ('5', ['1', 1]), ('2', ['3', 2]), ('5', ['3', 2])]
# Known_Paths= [('3', ['1', 1]), ('1', ['-', '0']), ('5', ['1', 1]), ('3', ['1', 1]), ('5', ['1', 1]), ('2', ['3', 2]), ('5', ['3', 2])]
# Known_Paths after reducing = [('5', ['1', 1]), ('1', ['-', '0']), ('3', ['1', 1]), ('2', ['3', 2])]

    known_paths.cache()
    path=[destination]
    for i in range(known_paths.lookup(destination)[0][1]):
        r=known_paths.lookup(destination)[0][0]
        path.insert(0,r)
        destination=r
    
    print("Path = ", path)
    finalpath=sc.parallelize(path)
    finalpath.saveAsTextFile(output + '/path')

if __name__ == '__main__':
    conf = SparkConf().setAppName('shortest_path')
    sc = SparkContext(conf=conf)
    sc.setLogLevel('WARN')
    assert sc.version >= '2.4'  # make sure we have Spark 2.4+
    inputs = sys.argv[1]
    output = sys.argv[2]
    source = sys.argv[3]
    destination = sys.argv[4]
    main(inputs, output, source, destination)

