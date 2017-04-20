package com.test.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class Sort {
    private static int time = 0;

    public static class TestMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new IntWritable(Integer.parseInt(value.toString())), new IntWritable(1));
        }
    }

    public static class TestReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        int count = 1;

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable value : values) {
                context.write(new IntWritable(count++), key);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "st join");
        job.setJarByClass(Sort.class);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path("input4"));
        Path output = new Path("output");
        output.getFileSystem(job.getConfiguration()).delete(output, true);
        FileOutputFormat.setOutputPath(job, output);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
