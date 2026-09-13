import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.TreeSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2Analysis{

    //tokenizer
    private static String[] tokenize(String line){
        return line.toLowerCase().split("[^a-z]+");
    }

    //fields
    private static final int FIELD_LINE_NUMBER = 0;
    private static final int FIELD_FIRST_NAME = 1;
    private static final int FIELD_LAST_NAME = 2;
    private static final int FIELD_CITY = 4;
    private static final int FIELD_STATE = 5;

    //for part b: word should be 12 characters long
    private static final int MIN_LONG_WORD_LENGTH = 12;
    private static final Text SINGLE_KEY = new Text("result");

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, LongWritable> {
        private final Text word = new Text();
        private final LongWritable lineNumberOut = new LongWritable();

        public void map(Object key, Text value, Context context)
        throws IOException, InterruptedException {
            //byte offset
            String line = value.toString();
 
            //comma seperation
            String[] fields = line.split(",", -1);
 
            if (fields.length <= FIELD_STATE){
                return;
            }

            long lineNumber;
            try {
                lineNumber = Long.parseLong(fields[FIELD_LINE_NUMBER].trim());
            } catch (NumberFormatException e) {
                return;
            }
            lineNumberOut.set(lineNumber);
            
            //tokenize all fields
            int[] fieldsToIndex = { FIELD_FIRST_NAME, FIELD_LAST_NAME, FIELD_CITY, FIELD_STATE };
            for (int fieldIndex : fieldsToIndex) {
                for (String token : tokenize(fields[fieldIndex])) {
                    if (token.isEmpty()) {
                        continue;
                    }
                    word.set(token);
                    context.write(word, lineNumberOut);
                }
            }
        }
    }

      public static class InvertedIndexReducer extends Reducer<Text, LongWritable, Text, Text> {
        private final Text result = new Text();
 
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // reduces duplicate words and sort numerically
            TreeSet<Long> lineNumbers = new TreeSet<>();
            for (LongWritable val : values) {
                lineNumbers.add(val.get());
            }
 
            // formatting to comma+single space
            StringBuilder sb = new StringBuilder();
            boolean first = true;
            for (Long n : lineNumbers) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(n);
                first = false;
            }
            result.set(sb.toString());
            context.write(key, result);
        }
    }

    public static class LongWordMapper extends Mapper<Object, Text, Text, Text> {
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Parsing Q2A output format
            String line = value.toString();
            String[] parts = line.split("\t", 2);
            //if malformed
            if (parts.length != 2) {
                return;
            }
 
            String word = parts[0];
            if (word.length() < MIN_LONG_WORD_LENGTH) {
                return;
            }

            //find frequency
            String lineList = parts[1].trim();
            int count;
            if (lineList.isEmpty()) {
                count = 0;
            } else {
                //counting tokens after splitting on comma
                count = lineList.split(",").length;
            }
 
            context.write(SINGLE_KEY, new Text(word + "," + count));
        }
    }

    public static class LongWordCombiner extends Reducer<Text, Text, Text, Text> {
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String bestWord = null;
            int bestCount = -1;
 
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String word = parts[0];
                int count = Integer.parseInt(parts[1]);
                if (count > bestCount) {
                    bestCount = count;
                    bestWord = word;
                }
            }
 
            if (bestWord != null) {
                context.write(SINGLE_KEY, new Text(bestWord + "," + bestCount));
            }
        }
    }

        public static class LongWordReducer extends Reducer<Text, Text, Text, LongWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //reducer picks best word
            String bestWord = null;
            long bestCount = -1;
 
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String word = parts[0];
                long count = Long.parseLong(parts[1]);
                if (count > bestCount) {
                    bestCount = count;
                    bestWord = word;
                }
            }
 
            //formatting for one line word <TAB> count
            if (bestWord != null) {
                context.write(new Text(bestWord), new LongWritable(bestCount));
            }
        }
    }

        //job for part a
        private static Job buildInvertedIndexJob(Configuration conf, String in, String out) throws IOException {
        Job job = Job.getInstance(conf, "Q2A InvertedIndex");
        
        job.setJarByClass(Q2Analysis.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        
        return job;
    }
 
    //job for part b
    private static Job buildLongWordJob(Configuration conf, String in, String out) throws IOException {
        Job job = Job.getInstance(conf, "Q2B MostFrequentLongWord");
        
        job.setJarByClass(Q2Analysis.class);
        job.setMapperClass(LongWordMapper.class);
        job.setCombinerClass(LongWordCombiner.class);
        job.setReducerClass(LongWordReducer.class);
        job.setNumReduceTasks(1);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        
        return job;
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
 
        if (otherArgs.length != 3) {
            System.err.println("Usage: Q2Analysis <in> <out> <part>");
            System.err.println("part: A (InvertedIndex) or B (MostFrequentLongWord)");
            System.exit(2);
        }
 
        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];
        String part = otherArgs[2];
 
        Job job;
        if (part.equals("A")) {
            job = buildInvertedIndexJob(conf, inputPath, outputPath);
        } else if (part.equals("B")) {
            job = buildLongWordJob(conf, inputPath, outputPath);
        } else {
            System.err.println("Unknown part: " + part);
            System.exit(2);
            return;
        }
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}