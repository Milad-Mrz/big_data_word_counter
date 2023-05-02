# Remove output folders from previous runs
rm -r ./p1a1-output ./p1a2-output ./p1a3-output

# Compile Java files
cd ./java_files
javac -classpath $(echo $HADOOP_HOME/share/hadoop/common/*.jar $HADOOP_HOME/share/hadoop/mapreduce/*.jar | tr ' ' ':') *.java
jar -cvf ./HadoopWordCount.jar *.class
cd ..

# Run Hadoop Java-API jobs
hadoop jar ./java_files/HadoopWordCount.jar P1A1_HadoopWordCount ./Data/enwiki-articles/AA ./p1a1-output
hadoop jar ./java_files/HadoopWordCount.jar P1A2_HadoopWordPairs ./Data/enwiki-articles/AA ./p1a2-output
hadoop jar ./java_files/HadoopWordCount.jar P1A3_HadoopWordStripes ./Data/enwiki-articles/AA ./p1a3-output
