from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import datetime
import os
import gzip
import re
import datetime
import sys

                    
def main(input_dir, table_name):
    #Creating Table Incase Doesnt Exist
    session.execute('CREATE TABLE IF NOT EXISTS ' + table_name + ' (uid UUID, host TEXT, datetime TIMESTAMP, path TEXT, bytes INT, PRIMARY KEY(host, uid));')

    #Batch Variables
    insert_query = session.prepare('INSERT INTO ' + table_name + ' (uid, host, datetime, path, bytes) VALUES (uuid(), ?, ?, ?, ?)')
    batch = BatchStatement(consistency_level=1)
    batch_size = 300

    #Define Regular Expression and Counter
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    counter = 0

    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            for line in logfile:
                log = re.match(line_re, line)
                if (log!=None):
                    host_name, date_time, req_path, bytes_trans = log.groups()
                    correct_date = datetime.datetime.strptime(date_time, "%d/%b/%Y:%H:%M:%S")
                    print("Batch: " + str(counter) + " " + host_name, date_time, req_path, bytes_trans )
                    counter = counter + 1

                    batch.add(insert_query, (host_name, correct_date, req_path, int(bytes_trans)))
                    if counter == batch_size:
                        print("Batch Full")
                        session.execute(batch)
                        batch.clear()
                        counter = 0 

            # After the last set of lines are added to the batch, execute to send the last set
            session.execute(batch)
            batch.clear()
            
                                          


if (__name__ == '__main__'):
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table_name = sys.argv[3]

    cluster = Cluster(['node1.local', 'node2.local'])
    session = cluster.connect(keyspace)

    main(input_dir, table_name)