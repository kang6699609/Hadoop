自定义map和reduce完成wordcount统计
启动步骤：
将程序打包放到hadoop集群上，然后将文件上传到hdfs上
启动命令：
hadoop jar HDFS-1.0-SNAPSHOT.jar com.kk.mapreduce.WordcountDriver /kk/wc.txt  /kk/a.txt

遇到问题：
1、没有指定文件的输出路径
报错：
Exception in thread "main" java.lang.ArrayIndexOutOfBoundsException: 1
	at com.kk.mapreduce.WordcountDriver.main(WordcountDriver.java:41)
2、机器内存不足
报错：
Exception in thread "main" java.io.IOException: org.apache.hadoop.yarn.exceptions.YarnException: Failed to submit application_1594087028119_0006 to YARN : Application application_1594087028119_0006 submitted by user root to unknown queue: default
