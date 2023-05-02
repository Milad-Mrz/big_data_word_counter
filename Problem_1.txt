Problem-1: Java-API Hadoop                                                                    Milad Mirzaei - 0210941626
------------------------------------------------------------------------------------------------------------------------
Part-A:
- How to run the files:
1- change directory to Problem_1 and Place "Data" folder in same directory
2- call Shell scripts "P1_A.sh" or the below commands:

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

- output samples and report:
These three codes, the original map function has been modified to filter data based on length and format. And reduce function count the occurance of the data.
Selected Data is containing lowercase letters with length between 5 and 25, or digits with length between 2 and 12.


P1A1_HadoopWordCount output:
--------------------------------------------------
data                frequency
--------------------------------------------------
...
99984629999	        1
99999999	        1
aaabbbccc	        2
aaata               2
...


P1A2_HadoopWordPairs output:
--------------------------------------------------
data                frequency
--------------------------------------------------
...
annual:technology	2
annual:televising	1
annual:temperature	26
annual:temperatures	2
...


P1A3_HadoopWordStripes output:
--------------------------------------------------
data                frequency
--------------------------------------------------
...
zoomorphhic	        org.apache.hadoop.io.MapWritable@78664d0
zooms	            org.apache.hadoop.io.MapWritable@e35ffe65
zoonotic	        org.apache.hadoop.io.MapWritable@20932e90
zooplankton	        org.apache.hadoop.io.MapWritable@d490f3c0
...


------------------------------------------------------------------------------------------------------------------------
Part-B:
- How to run the files:
1- change directory to Problem_1 and Place "Data" folder in same directory
2- call Shell scripts "P1_B.sh" or the below commands:

    # Remove output folders from previous runs
    rm -r ./p1b1-out1 ./p1b1-out2 ./p1b1-out3 ./p1b1-out4 ./p1b1-out5 ./p1b1-out6

    # Compile Java files
    cd ./java_files
    javac -classpath $(echo $HADOOP_HOME/share/hadoop/common/*.jar $HADOOP_HOME/share/hadoop/mapreduce/*.jar | tr ' ' ':') *.java
    jar -cvf ./HadoopWordCount.jar *.class
    cd ..

    # Run the Hadoop Java-API job
    hadoop jar ./java_files/HadoopWordCount.jar P1B_combined ./Data/enwiki-articles/AA ./p1b1-out1 ./p1b1-out2 ./p1b1-out3 ./p1b1-out4 ./p1b1-out5 ./p1b1-out6

- output samples, and report:
this java file contain 6 jobs,

The first and third jobs select and filter words with any length from raw data:

1st job output: (150,589 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
a	                161493
aa	                227
aaa	                26
aaabbbccc	        2
...


3rd job output: (1,716,035 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
...
they:called	        24
they:came	        34
they:campaigned    	3
they:can	        414
they:cannot	        49
...

The second and fourth jobs select data with a count of exactly 1,000.

2nd job output: (1 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
philosophy	        1000


4th job output: ( count_number == 1000 ) (0 data)*
--------------------------------------------------
data                frequency
--------------------------------------------------
-                   -

* fourth job output is empty due to simply lack of any pairs with frequency of exactly 1000.
However, if for example count_number is set to 100, the code works fine:

4th job output: ( count_number == 100 ) (6 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
alkali:metals	    100
brown:bears	        100
command:module	    100
early:years	        100
federal:government  100
total:number	    100


Jobs five and six select top 100 results with the highest number of occurrence:

5th job output: (100 data)
--------------------------------------------------
num        data             frequency
--------------------------------------------------
001        a	            161493
002        about	        7618
003        after	        12281
004        all	            12478
005        also	            20436
...
096        with	            55583
097        world	        8326
098        would	        9117
099        year	            5862
100        years	        7662


6th job output: (100 data)
--------------------------------------------------
num        data             frequency
--------------------------------------------------
001        according:to	    3696
002        after:the	    3743
003        and:and	        5249
004        and:his	        2956
005        and:in	        2217
...
096        well:as	        3353
097        when:the	        2378
098        which:is	        2815
099        with:a	        5844
100        with:the	        13179



------------------------------------------------------------------------------------------------------------------------
Part-C:
- How to run the files:
1- change directory to Problem_1 and Place "Data" folder in same directory
2- to run the Hadoop Java-API job for all directories, call Shell scripts "P1_C.sh"

- output samples, and report:
Same program has been used to analyse entire available data

1st job output: (561,702 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
a	                1736902
aa	                689
aaa	                230
aaaa	            10
aaaaa	            4
...


3rd job output: (9,788,921 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
a:a	                6146
a:aa	            4
a:aaa	            8
a:aaaaa	            1
...

The second and fourth jobs select data with a count of exactly 1,000.
2nd job output: (4 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
caves	            1000
dairy	            1000
mongol	            1000
rats	            1000


4th job output: ( Count number == 1000 ) (11 data)
--------------------------------------------------
data                frequency
--------------------------------------------------
a:campaign	        1000
a:nuclear	        1000
and:said	        1000
club:in	            1000
form:in	            1000
league:in	        1000
probability:of	    1000
the:independence	1000
the:opera	        1000
the:policy	        1000
to:recognize	    1000


Jobs five and six select top 100 results with the highest number of occurrence:
5th job output: (100 data)
--------------------------------------------------
num        data             frequency
--------------------------------------------------
001        a	            1736902
002        about	        80971
003        after	        140330
004        all	            130350
005        also	            214250
...
096        with	            593254
097        world	        85756
098        would	        102680
099        year	            73154
100        years	        86059


6th job output: (500 data)**
--------------------------------------------------
num        data             frequency
--------------------------------------------------
001        according:to	    3696
002        after:the	    3743
003        and:and	        5249
004        and:his	        2956
005        and:in	        2217
...
096        well:as	        3353
097        when:the	        2378
098        which:is	        2815
099        with:a	        5844
100        with:the	        13179

** 500 data happens due to configuration of Hadoop and number of parallel mappers.
it can be fixed by manually setting "mapreduce.job.maps" in Hadoop configuration.
by default, the number of mappers is determined by several factors such as the size of the input data,
the size of the HDFS block, and the number of available DataNodes in the cluster.


------------------------------------------------------------------------------------------------------------------------

- run-time comparison:
--------------------------------------------------
Jobs        Single directory        All directories
--------------------------------------------------
001         17  sec                 182 sec
002         1   sec                 2   sec
003         20  sec                 201 sec
004         3   sec                 20  sec
005         1   sec                 2   sec
006         3   sec                 9   sec
--------------------------------------------------
total       45  sec                 416 sec



------------------------------------------------------------------------------------------------------------------------
System Configuration:
CPU/GPU/APU: AMD Ryzen-5 4500U and Radeon Graphics
RAM: 16 GB
OS: Ubuntu 22.04.02







