build:
	${HADOOP_HOME}/bin/hadoop com.sun.tools.javac.Main ParallelSort.java
	jar cf ParallelSort.jar ParallelSort*.class

clean:
	rm -f *.class
	rm -f *.jar

clear_output:
	${HADOOP_HOME}/bin/hdfs dfs -rm -r -f /users/lbui3/output
	${HADOOP_HOME}/bin/hdfs dfs -rm -f /users/lbui3/partition

# turn permission on for all users. This doesn't relate to sorting. This helps one to easily clean work done earlier by another user.
permission:
	${HADOOP_HOME}/bin/hdfs dfs -chmod -R 777 /users/lbui3/

run_120: clean build clear_output
	${HADOOP_HOME}/bin/hadoop jar ParallelSort.jar ParallelSort /users/lbui3/input_120 /users/lbui3/output /users/lbui3/cache /users/lbui3/partition 120
	$(MAKE) permission

run_6k: clean build clear_output
	${HADOOP_HOME}/bin/hadoop jar ParallelSort.jar ParallelSort /users/lbui3/input_6k /users/lbui3/output /users/lbui3/cache /users/lbui3/partition 6000
	$(MAKE) permission

run_6m: clean build clear_output
	${HADOOP_HOME}/bin/hadoop jar ParallelSort.jar ParallelSort /users/lbui3/input_6m /users/lbui3/output /users/lbui3/cache /users/lbui3/partition 6000000
	$(MAKE) permission

run_60m: clean build clear_output
	${HADOOP_HOME}/bin/hadoop jar ParallelSort.jar ParallelSort /users/lbui3/input_60m /users/lbui3/output /users/lbui3/cache /users/lbui3/partition 60000000
	$(MAKE) permission

run_120m: clean build clear_output
	${HADOOP_HOME}/bin/hadoop jar ParallelSort.jar ParallelSort /users/lbui3/input_120m /users/lbui3/output /users/lbui3/cache /users/lbui3/partition 120000000
	$(MAKE) permission
