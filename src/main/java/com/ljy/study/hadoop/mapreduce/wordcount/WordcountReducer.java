package com.ljy.study.hadoop.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 本类设计：
 * 获取map进程传递过来的数据，这里的输入类型必须与map进行的输出类型一致；然后将处理结果用Map类型输出
 *
 * Reducer<Text, IntWritable, Text, IntWritable>
 * 第一个参数：从WordcountMapper处理后传递过来的单词类型（如hello）
 * 第二个参数：从WordcountMapper处理后传递过来的单词对应的值的类型（如该单词为hello,value就是对应的值1）
 * 第三个参数：在WordcountReducer处理后输出的键的类型（如单词hello）
 * 第四个参数：在WordcountReducer处理后输出的值的类型（如该单词为hello,value就是统计后的总次数）
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{

    int sum;//定义单词总数量
    IntWritable v = new IntWritable();

    //key是一个单词，values里面装有在mapreduce中map清洗数据后传递过来的相同单词的value集合（如单词为hello,value就是{1,1,1,1,1,1}）
    //map将每个单词对应的值集合处理完后，调用一次reduce
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        // 1 累加求和
        sum = 0;
        for (IntWritable count : values) {//将该单词对应的值的数量相加，获得单词总数量
            sum += count.get();
        }
        // 2 输出该单词的总数量
        v.set(sum);
        context.write(key,v);
    }
}
