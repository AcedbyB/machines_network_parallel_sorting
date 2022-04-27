Compile:
- $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main ParallelSort.java
- jar cf ParallelSort.jar ParallelSort*.class

Clean previous output before run:
- $HADOOP_HOME/bin/hdfs dfs -rm -r -f [path to ouput folder] # clean output folder
- $HADOOP_HOME/bin/hdfs dfs -rm -f [path to partition file] # clean partiton file

Run:
- $HADOOP_HOME/bin/hadoop jar ParallelSort.jar ParallelSort [path to input folder] [path to ouput folder] [path to cache file] [path to partition file] [input size]

Ex:
- $HADOOP_HOME/bin/hadoop com.sun.tools.javac.Main ParallelSort.java
- jar cf ParallelSort.jar ParallelSort*.class
- $HADOOP_HOME/bin/hdfs dfs -rm -r -f /users/lbui3/output
- $HADOOP_HOME/bin/hdfs dfs -rm -f /users/lbui3/partition
- $HADOOP_HOME/bin/hadoop jar ParallelSort.jar ParallelSort /users/lbui3/input_120 /users/lbui3/output /users/lbui3/cache /users/lbui3/partition 120

One can also use Makefile with predefined option, which I set up earlier, to run:
- Run with input 120 records: $make run_120
- Run with input 6000 records: $make run_6k
- Run with input 6 millions records: $make run_6m
- Run with input 60 millions records: $make run_60m
- Run with input 120 millions records: $make run_120m