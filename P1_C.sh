# Remove output folders from previous runs
rm -r ./p1c1-out1 ./p1c1-out2 ./p1c1-out3 ./p1c1-out4 ./p1c1-out5 ./p1c1-out6

# Create a new directory to store a copy of all Data
mkdir ./temp  # create a new directory

for dir in ./Data/enwiki-articles/*/  # iterate through each subdirectory in the directory
do
    dirname=$(basename "$dir")  # get the name of the subdirectory
    for file in "$dir"/*  # iterate through each file in the subdirectory
    do
        filename=$(basename "$file")  # get the filename without the path
        new_filename="${filename}_${dirname}"  # create the new filename
        cp -r "$file" "./temp/$new_filename"  # copy the file to the new folder with the new filename
    done
done

# Compile Java files (if not compiled in previous runs)
cd ./java_files
javac -classpath $(echo $HADOOP_HOME/share/hadoop/common/*.jar $HADOOP_HOME/share/hadoop/mapreduce/*.jar | tr ' ' ':') *.java
jar -cvf ./HadoopWordCount.jar *.class
cd ..

# Run the Hadoop Java-API job for all directories at once
hadoop jar ./java_files/HadoopWordCount.jar P1B ./temp ./p1c1-out1 ./p1c1-out2 ./p1c1-out3 ./p1c1-out4 ./p1c1-out5 ./p1c1-out6

# Remove the temp directory
rm -r ./temp
