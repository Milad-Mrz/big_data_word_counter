import java.io.IOException;
import java.util.Collections;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class P1B extends Configured implements Tool {
    // First job: The mapper filters words while the reducer counts them.
    public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\\W+");
            for (String word : splitLine) {
                word = word.toLowerCase();
                // Here, the input words are being filtered based on their length and format.
                // Words containing only lowercase letters with length between 5 and 25.
                if (word.matches("^[a-z]+$") ) {
                    context.write(new Text(word), new IntWritable(1));
                }
            }
        }
    }

    public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }
    // As the raw data is stored in multiple documents, a second task will be created to examine the frequency of occurrence in the output of the first job.
    // 2nd job: The Map2 class will read the output of the first job and split each line into text and frequency. It will then output the frequency as the key and the text as the value.
    public static class Map2 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\\W+");
            String wtext = splitLine[0].toLowerCase();
            int wfreq = Integer.parseInt(splitLine[1]);
            //System.out.println("text: "+wtext+" - number: "+wfreq);
            context.write(new Text(wtext), new IntWritable(wfreq));


        }
    }
    // Reduce2 class will receive the output from the FilterMap class and simply look for "count_number" and write it to the final output.
    public static class Reduce2 extends Reducer<Text, IntWritable, Text, IntWritable> {
        // defining the desired frequency count to search for
        private static final int count_number = 1000;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int wfreq = 0;
            for (IntWritable value : values)
                wfreq += value.get();
            if (wfreq == count_number) {
                    context.write(key, new IntWritable(wfreq));
                }
        }
    }

    // 3rd job: The mapper filters word pairs while the reducer counts them.
    public static class Map3 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pair = new Text();
        private Text lastWord = new Text();
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split(" ");
            for (String word : splitLine) {
                word = word.toLowerCase();
                // Here, the input words are being filtered based on their length and format.
                // Words containing only lowercase letters with length between 5 and 25.
                if (word.matches("^[a-z]+$")) {
                    if (lastWord.getLength() > 0) {
                        pair.set(lastWord + ":" + word);
                        context.write(pair, one);
                    }
                    lastWord.set(word);
                }
            }
        }
    }

    public static class Reduce3 extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values)
                sum += value.get();
            context.write(key, new IntWritable(sum));
        }
    }

    // As the raw data is stored in multiple documents, a second task will be created to examine the frequency of occurrence in the output of the first job.
    // 4th job: The Map4 class will read the output of the first job and split each line into text and frequency. It will then output the frequency as the key and the text as the value.
    public static class Map4 extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\\W+");
            String wtext1 = splitLine[0].toLowerCase();
            String wtext2 = splitLine[1].toLowerCase();
            int wfreq = Integer.parseInt(splitLine[2]);
            context.write(new Text(wtext1+":"+wtext2), new IntWritable(wfreq));
        }
    }
    // The Reduce class will receive the output from the FilterMap class and simply look for "count_number" and write it to the final output.
    public static class Reduce4 extends Reducer<Text, IntWritable, Text, IntWritable> {
        // defining the desired frequency count to search for
        private static final int count_number = 1000;
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int wfreq = 0;
            for (IntWritable value : values)
                wfreq += value.get();
            if (wfreq == count_number) {
                context.write(key, new IntWritable(wfreq));
            }
        }
    }

    // As the raw data is stored in multiple documents, a second task will be created to examine the frequency of occurrence in the output of the first job.
    // 5th job: The Map5 class will read the output of the first job and split each line into text and frequency. It will then output the frequency as the key and the text as the value.
    public static class Map5 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private TreeMap<Integer, String> topWordsMap = new TreeMap<Integer, String>(Collections.reverseOrder());
        private static final int top_frequency = 100;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\\W+");
            String word_text = splitLine[0].toLowerCase();
            int word_freq = Integer.parseInt(splitLine[1]);
            // add the word to the topWordsMap
            topWordsMap.put(word_freq, word_text);
            // keep only the top 100 words in the topWordsMap
            if (topWordsMap.size() > top_frequency) {
                topWordsMap.remove(topWordsMap.lastKey());
            }
        }
        // This void is part of FilterMap class and it will simply emit the output.
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // emit the top 100 words
            for (Integer key : topWordsMap.keySet()) {
                String value = topWordsMap.get(key);
                context.write(new Text(value), new IntWritable(key) );
            }
        }
    }

    // As the raw data is stored in multiple documents, a second task will be created to examine the frequency of occurrence in the output of the first job.
    // 6th job: The Map6 class will read the output of the first job and split each line into text and frequency. It will then output the frequency as the key and the text as the value.
    public static class Map6 extends Mapper<LongWritable, Text, Text, IntWritable> {
        private TreeMap<Integer, String> topWordsMap = new TreeMap<Integer, String>(Collections.reverseOrder());
        private static final int top_frequency = 100;
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splitLine = value.toString().split("\\W+");
            String wtext1 = splitLine[0].toLowerCase();
            String wtext2 = splitLine[1].toLowerCase();
            String word_text = wtext1+":"+wtext2;
            int word_freq = Integer.parseInt(splitLine[2]);
            // add the word to the topWordsMap
            topWordsMap.put(word_freq, word_text);
            // keep only the top 100 words in the topWordsMap
            if (topWordsMap.size() > top_frequency) {
                topWordsMap.remove(topWordsMap.lastKey());
            }
        }

        // This void is part of FilterMap class and it will simply emit the output.
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // emit the top 100 words
            for (Integer key : topWordsMap.keySet()) {
                String value = topWordsMap.get(key);
                context.write(new Text(value), new IntWritable(key) );
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job1 = Job.getInstance(getConf(), "word_count");
        job1.setJarByClass(getClass());
        job1.setMapperClass(Map1.class);
        job1.setCombinerClass(Reduce1.class);
        job1.setReducerClass(Reduce1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        Job job2 = Job.getInstance(getConf(), "word_select");
        job2.setJarByClass(getClass());
        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/part-r-00000" ));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        Job job3 = Job.getInstance(getConf(), "Word_Pairs_Counter");
        job3.setJarByClass(getClass());
        job3.setMapperClass(Map3.class);
        job3.setCombinerClass(Reduce3.class);
        job3.setReducerClass(Reduce3.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));

        Job job4 = Job.getInstance(getConf(), "word_select_pair");
        job4.setJarByClass(getClass());
        job4.setMapperClass(Map4.class);
        job4.setReducerClass(Reduce4.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[3] + "/part-r-00000" ));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));

        Job job5 = Job.getInstance(getConf(), "select_top_100_words");
        job5.setJarByClass(getClass());
        job5.setMapperClass(Map5.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job5, new Path(args[1] + "/part-r-00000" ));
        FileOutputFormat.setOutputPath(job5, new Path(args[5]));

        Job job6 = Job.getInstance(getConf(), "select_top_100_words_pairs");
        job6.setJarByClass(getClass());
        job6.setMapperClass(Map6.class);
        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job6, new Path(args[3] + "/part-r-00000" ));
        FileOutputFormat.setOutputPath(job6, new Path(args[6]));


        int success = 0;
        if (job1.waitForCompletion(true)) {
            if (job2.waitForCompletion(true)) {
                if (job3.waitForCompletion(true)) {
                    if (job4.waitForCompletion(true)) {
                        if (job5.waitForCompletion(true)) {
                            success = job6.waitForCompletion(true) ? 0 : 1;
                        }
                    }
                }
            }
        }
        return success;
    }

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new Configuration(), new P1B(), args);
        System.exit(ret);
    }
}

//------------------------------------------------------------------------------------------------------------------------------------------
