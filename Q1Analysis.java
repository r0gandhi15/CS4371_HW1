import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
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

public class Q1Analysis{

    //tokenizer
    private static String[] tokenize(String line){
        return line.toLowerCase().split("[^a-z]+");
    }

    // Part A
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] tokens = value.toString().toLowerCase().split("[^a-z]+");
            //using byte offset of the line
            for (String token : tokens) {
                if(token.isEmpty()){
                    continue;
                }
                word.set(token);
                //emit (word, 1) per occurence
                context.write(word, one);
            }
        }
    }

    public static class WordCountReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // group all the 1s for a given word together and add them up
            int sum = 0;
            for (IntWritable val : values) sum += val.get();
            context.write(key, new IntWritable(sum));
        }
    }

    //Part B
    public static class TargetWordsMapper extends Mapper<Object, Text, Text, IntWritable>{
         private static final Set<String> TARGETS = new HashSet<>(Arrays.asList("ahab", "captain", "harpoon"));
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().toLowerCase().split("[^a-z]+");
            for (String token : tokens) {
                if (token.isEmpty()){
                continue;
                }
                if (TARGETS.contains(token)) {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }
    }
    
    public static class TargetWordsReducer  extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            //sum whatever arrives for each key
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    //Part C
    public static class PatternMapper extends Mapper<Object, Text, Text, Text>{
        private Text outKey = new Text();
        private Text outVal = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().toLowerCase().split("[^a-z]+");
            for (String token : tokens) {
                if (token.isEmpty()){

                continue;
                }
                int len = token.length();
                char lastChar = token.charAt(len - 1);
                outKey.set(len + "," + lastChar);
                outVal.set(token);
                context.write(outKey, outVal);
            }
        }
    }

    public static class PatternReducer  extends Reducer<Text, Text, Text, IntWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Set<String> distinctWords = new HashSet<>();
        for (Text val : values) {
            distinctWords.add(val.toString());
        }
        context.write(key, new IntWritable(distinctWords.size()));
        }
    }
    
     //helper methods
     private static Job buildWordCountJob(Configuration conf, String in, String out) throws IOException{
        Job job = Job.getInstance(conf, "Q1A WordCount");

        job.setJarByClass(Q1Analysis.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        
        return job;
    }

    private static Job buildTargetWordsJob(Configuration conf, String in, String out) throws IOException {
        Job job = Job.getInstance(conf, "Q1B TargetWords");
        job.setJarByClass(Q1Analysis.class);
        job.setMapperClass(TargetWordsMapper.class);
        job.setReducerClass(TargetWordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        return job;
    }

    private static Job buildPatternJob(Configuration conf, String in, String out) throws IOException{
        Job job = Job.getInstance(conf, "Q1C Pattern");
        job.setJarByClass(Q1Analysis.class);
        job.setMapperClass(PatternMapper.class);
        job.setReducerClass(PatternReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out));
        return job;
    }


    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3){
            System.err.println("Usage: Q1Analysis <in> <out> <part>");
            System.err.println("part: A (WordCount), B (TargetWords), or C (Pattern)");
            System.exit(2);
        }

        String inputPath = otherArgs[0];
        String outputPath = otherArgs[1];
        String part = otherArgs[2];

        Job job;
        if (part.equals("A")){
            job = buildWordCountJob(conf, inputPath, outputPath);
        }
        else if (part.equals("B")){
            job = buildTargetWordsJob(conf, inputPath, outputPath);
        }
        else if(part.equals("C")){
            job = buildPatternJob(conf, inputPath, outputPath);
        }
        else{
            System.err.println("Unknown part: " + part);
            System.exit(2);
            return;
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
    }
}
