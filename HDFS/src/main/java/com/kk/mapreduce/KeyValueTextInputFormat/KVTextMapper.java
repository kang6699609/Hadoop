package com.kk.mapreduce.KeyValueTextInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author yuanxiaokang
 * data 2020/9/29
 * 描述：
 */
public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {
    LongWritable v = new LongWritable(1);
    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        context.write(key,v);
    }
}
