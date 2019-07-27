package com.ljy.study.hadoop.mapreduce.wordcount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 定义maptask要做的事，并且将结果输出给reduce
 * Mapper<LongWritable, Text, Text, IntWritable>
 * 第一个参数：输入文本中的每行数据的偏移量
 * 第二个参数：输入文件中每行数据的类型
 * 第三个参数：输出文本中的键值的类型：单词（清洗后的数据）
 * 第四个参数：输出文本中的值的类型：单词个数（清洗后的数据，未统计）
 */

public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    Text k = new Text();
    IntWritable v = new IntWritable();


    //定义map的业务逻辑，maptask做什么由我们来设计
    //每读取一行调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // 1 获取一行数据
        String line = value.toString();
        // 2 将单词进行切割，如hello,world切割为数组：{"hello","world"}
        String[] words = line.split(",");
        // 3 将处理后的数据输出，后面给reduc处理
        for (String word : words) {
            k.set(word);//包装为hadoop要求的输出类型
            v.set(1);//包装为hadoop要求的输出类型，切割后的每个单词默认是1个
            context.write(k, v);
        }
    }
}

