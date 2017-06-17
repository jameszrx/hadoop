1. the rdd files are in hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/rdd

2. the page rank files are in hadoop-mapreduce-project/hadoop-mapreduce-examples/src/main/java/org/apache/hadoop/examples

to build the project

$ mvn install -DskipTests

replace the hadoop-mapreduce-client-core-2.8.0.jar and hadoop-mapreduce-examples-2.8.0.jar in the hadoop path.

To run the pagerank on android:

$ hadoop jar hadoop-mapreduce-examples-2.8.0.jar pagerank