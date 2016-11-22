package com.mark.service.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by lulei on 2016/11/22.
 */
public class Temperature {

    static class TempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 打印样本: Before Mapper: 0, 2000010115
            System.out.print("Before Mapper: " + key + ", " + value);
            String line = value.toString();
            String year = line.substring(0, 4);
            int temperature = Integer.parseInt(line.substring(8));
            context.write(new Text(year), new IntWritable(temperature));
            // 打印样本: After Mapper:2000, 15
            System.out.println(
                    "======" +
                            "After Mapper:" + new Text(year) + ", " + new IntWritable(temperature));

        }
    }

    static class TempReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int maxValue = Integer.MIN_VALUE;
            StringBuffer sb = new StringBuffer();
            for (IntWritable value : values) {
                maxValue = Math.max(maxValue, value.get());
                sb.append(value).append(", ");
            }
            // 打印样本： Before Reduce: 2000, 15, 23, 99, 12, 22,
            System.out.print("Before Reduce: " + key + ", " + sb.toString());
            context.write(key, new IntWritable(maxValue));
            // 打印样本： After Reduce: 2000, 99
            System.out.println(
                    "======" +
                            "After Reduce: " + key + ", " + maxValue);


        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String dst = "hdfs://hadooptest:9000/mark/test/input.txt";
        String dstOut = "hdfs://hadooptest:9000/mark/out";
        Configuration configuration = new Configuration();
        configuration.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", LocalFileSystem.class.getName());


        Job job = new Job(configuration);
        //如果需要打成jar运行，需要下面这句
        //job.setJarByClass(NewMaxTemperature.class);

        //job执行作业时输入和输出文件的路径
        FileInputFormat.addInputPath(job, new Path(dst));
        FileOutputFormat.setOutputPath(job, new Path(dstOut));

        //指定自定义的Mapper和Reducer作为两个阶段的任务处理类
        job.setMapperClass(TempMapper.class);
        job.setReducerClass(TempReducer.class);

        //设置最后输出结果的Key和Value的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //执行job，直到完成
        job.waitForCompletion(true);
        System.out.println("Finished");


    }
}
