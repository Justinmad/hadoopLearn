package com.test.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Dedup {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String s = value.toString().split(" ")[1];
            context.write(new Text(s), value);
        }
    }

    public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text text : values) {
                context.write(key, text);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "dedup");
        job.setJarByClass(Dedup.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        Path output = new Path("output");
        output.getFileSystem(job.getConfiguration()).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
