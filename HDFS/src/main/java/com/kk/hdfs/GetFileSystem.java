package com.kk.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * @author yuanxiaokang
 * data 2020/9/27
 * 描述：
 */
public class GetFileSystem {
    /**
     * 获取filesystem的几种方式
     */
    protected FileSystem getFileSystem1(){
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop5:8020");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  fileSystem;
    }

    protected FileSystem getFileSystem2() {
        FileSystem fileSystem = null;
        try {
            try {
                fileSystem = FileSystem.get(new URI("hdfs://hadoop5:8020"), new Configuration(),"root");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    protected FileSystem getFileSystem3() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://hadoop5:8020");
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.newInstance(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }

    protected FileSystem getFileSystem4(){
        FileSystem fileSystem = null;
        try {
            fileSystem = FileSystem.newInstance(new URI("hdfs://hadoop5:8020"), new Configuration());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return fileSystem;
    }
}
