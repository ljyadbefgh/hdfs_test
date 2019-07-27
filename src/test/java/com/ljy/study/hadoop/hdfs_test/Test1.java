package com.ljy.study.hadoop.hdfs_test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class Test1 {

    @Test
    public void getFileSystem() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), configuration);
        System.out.println(fileSystem.toString());
    }

    /**
     * hdfs文件上传,将本地的文件上传到指定文件夹
     * 结果：成功
     * @author  Ljy
     */
    @Test
    public void putData() throws  Exception{
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.52.110:8020"), new Configuration());
        fileSystem.copyFromLocalFile(new Path("file:///c:\\config.ini"),new Path("/myhdfs"));
        fileSystem.close();
    }


    /**
     * 递归遍历官方提供的API版本
     * @throws Exception
     */
    @Test
    public void listMyFiles()throws Exception{
        //获取fileSystem类
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://192.168.52.110:8020"), new Configuration());
        //获取RemoteIterator 得到所有的文件或者文件夹，第一个参数指定遍历的路径，第二个参数表示是否要递归遍历
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath().toString());
        }
        fileSystem.close();
    }



}
