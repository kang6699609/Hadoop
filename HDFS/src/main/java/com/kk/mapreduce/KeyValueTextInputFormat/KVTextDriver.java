package com.kk.mapreduce.KeyValueTextInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author yuanxiaokang
 * data 2020/9/29
 * 描述：
 */
public class KVTextDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"d:/test/shopping.txt", "d:/output/kv"};
        Configuration configuration = new Configuration();
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
        // 1 获取job对象
        Job job = Job.getInstance();
        // 2 设置jar包位置，关联mapper和reducer
        job.setJarByClass(KVTextDriver.class);
        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);
        // 3 设置map输出kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //4 设置最终输出的key value 类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //5 设置输入的路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        // 设置输入格式
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //6 设置输出的路径
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 7 提交job
        job.waitForCompletion(true);
    }
}

