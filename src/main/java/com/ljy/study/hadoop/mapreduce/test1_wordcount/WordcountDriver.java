package com.ljy.study.hadoop.mapreduce.test1_wordcount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 在本机运行-windows
 *
 * 如果要把项目打包到项目中运行，执行以下步骤
 * （1）将程序打成jar包，然后拷贝到hadoop集群中
 * （2）启动hadoop集群
 * （3）执行wordcount程序
 * [bigdata@hadoop102 software]$ hadoop jar  wc.jar  com.ljy.study.hadoop.mapreduce.test1_wordcount.WordcountDriver /user/bigdata/input /user/bigdata/output1
 *
 * hadoop jar  hdfs_test.jar  com.ljy.study.hadoop.mapreduce.test1_wordcount.WordcountDriver /myhdfs/wordcount/input /myhdfs/wordcount/ouput
 */


public class WordcountDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 设置jar加载路径，放到服务器的时候记得去掉注释
        job.setJarByClass(WordcountDriver.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 指定reduce最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        // 6 设置输入和输出路径
        //在本地运行,如果在本地运行请使用下面代码
        //FileInputFormat.setInputPaths(job, new Path("E:\\hadoop_sucai\\wordcount\\input"));//要输入文件的目录
        //FileOutputFormat.setOutputPath(job, new Path("E:\\hadoop_sucai\\wordcount\\output"));//输出结果的目录，必须不存在，否则会出错

        //上传到服务器运行,如果在本地运行请注释掉下面代码
        FileInputFormat.setInputPaths(job, new Path(args[0]));//要输入文件的目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//输出结果的目录，必须不存在，否则会出错

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
