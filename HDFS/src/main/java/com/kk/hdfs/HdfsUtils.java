package com.kk.hdfs;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;

/**
 * @author yuanxiaokang
 * data 2020/9/27
 * 描述：
 */
public class HdfsUtils {
    @Test
    /**
     * 从HDFS下载文件到本地磁盘
     * 使用url的方式访问数据
     */
    public void demo1() throws Exception {
        //第一步：注册hdfs的url
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        //获取文件输入流
        InputStream inputStream = new
                URL("hdfs://hadoop5:8020/kk/wc.txt").openStream();
        //获取文件输出流
        FileOutputStream outputStream = new FileOutputStream(new
                File("D:\\wc.txt"));
        //实现文件的拷贝
        IOUtils.copy(inputStream, outputStream);
        //关闭流
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }


    /**
     * 通过文件系统操作hdfs文件
     */

    /**
     * 获取hdfs的文件列表
     */
    public static void listMyFiles() {
        FileSystem fileSystem = new GetFileSystem().getFileSystem1();
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = null;
        try {
            locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/kk"), true);
        } catch (IOException e) {
            e.printStackTrace();
        }
        while (true) {
            try {
                if (!locatedFileStatusRemoteIterator.hasNext()) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            LocatedFileStatus next = null;
            try {
                next = locatedFileStatusRemoteIterator.next();
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println(next.getPath().toString());
        }
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * hdfs上创建文件夹
     */
    public static void mkdirs() {
        FileSystem fileSystem = new GetFileSystem().getFileSystem1();
        boolean mkdirs = false;
        try {
            mkdirs = fileSystem.mkdirs(new Path("/kk/cc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(mkdirs);
    }

    /**
     * 通过filesystem下载hdfs文件
     */
    public static void getFileToLocal() {
        FileSystem fileSystem = new GetFileSystem().getFileSystem1();
        try {
            fileSystem.copyToLocalFile(new Path("/kk/wc.txt"), new Path("/e:"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过filesystem上传hdfs文件
     */
    public static void putFileToHDFS() {
        FileSystem fileSystem = new GetFileSystem().getFileSystem1();
        try {
            fileSystem.copyFromLocalFile(new Path("file:///E:\\大数据文档\\学习文档\\Apache+Flink特刊（正式电子版）.pdf"), new Path("/kk/cc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void mergeFile() throws Exception {
        //获取分布式文件系统
        FileSystem fileSystem = FileSystem.get(new
                URI("hdfs://hadoop5:8020"), new Configuration(), "root");
        FSDataOutputStream outputStream = fileSystem.create(new
                Path("/bigfile.txt"));
        //获取本地文件系统
        LocalFileSystem local = FileSystem.getLocal(new Configuration());
        //通过本地文件系统获取文件列表，为一个集合
        FileStatus[] fileStatuses = local.listStatus(new
                Path("file:///E:\\大数据文档\\学习文档"));
        for (FileStatus fileStatus : fileStatuses) {
            FSDataInputStream inputStream = local.open(fileStatus.getPath());
            IOUtils.copy(inputStream, outputStream);
            IOUtils.closeQuietly(inputStream);
        }
        IOUtils.closeQuietly(outputStream);
        local.close();
        fileSystem.close();
    }

    public static void main(String[] args) {
        try {
            putFileToHDFS();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

