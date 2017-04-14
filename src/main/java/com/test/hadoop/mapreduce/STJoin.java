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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class STJoin {
    private static int time = 0;

    public static class TestMapper extends Mapper<Object, Text, Text, Text> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(" ");
            String child = split[0];
            String parent = split[1];
            if (split[0].compareTo("child") != 0) {
                context.write(new Text(split[0]), new Text("2" + parent));
                context.write(new Text(split[1]), new Text("1" + child));
            }
        }
    }

    public static class TestReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (time == 0) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            List<Text> grandchild = new ArrayList<>(10);
            List<Text> grandparent = new ArrayList<>(10);
            // 此处迭代器每次返回的value是同一个Text对象，它的值每次next方法被调用时被修改
            for (Text value : values) {
                char type = value.toString().charAt(0);
                if (type == '1') {
                    grandchild.add(new Text(value.toString().substring(1)));
                }
                if (type == '2') {
                    grandparent.add(new Text(value.toString().substring(1)));
                }
            }
            Collections.sort(grandchild);
            if (!grandchild.isEmpty() && !grandparent.isEmpty()) {
                for (Text text : grandchild) {
                    for (Text text1 : grandparent) {
                        context.write(text, text1);
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "root");
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, "st join");
        job.setJarByClass(STJoin.class);
        job.setMapperClass(TestMapper.class);
        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input2"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
