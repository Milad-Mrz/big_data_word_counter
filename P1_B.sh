# Remove output folders from previous runs
rm -r ./p1b1-out1 ./p1b1-out2 ./p1b1-out3 ./p1b1-out4 ./p1b1-out5 ./p1b1-out6

# Compile Java files (if not compiled in previous runs)
cd ./java_files
javac -classpath $(echo $HADOOP_HOME/share/hadoop/common/*.jar $HADOOP_HOME/share/hadoop/mapreduce/*.jar | tr ' ' ':') *.java
jar -cvf ./HadoopWordCount.jar *.class
cd ..

# Run the Hadoop Java-API job
hadoop jar ./java_files/HadoopWordCount.jar P1B ./Data/enwiki-articles/AA ./p1b1-out1 ./p1b1-out2 ./p1b1-out3 ./p1b1-out4 ./p1b1-out5 ./p1b1-out6

