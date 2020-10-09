package com.kk.mapreduce.KeyValueTextInputFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author yuanxiaokang
 * data 2020/9/29
 * 描述：
 */
public class KVTextReducer extends Reducer<Text, LongWritable, Text,LongWritable> {

    LongWritable v = new LongWritable();
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0L;
        for (LongWritable value:values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key,v);
    }
}
