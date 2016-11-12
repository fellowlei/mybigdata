package com.mark.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;

/**
 * Created by lulei on 2016/11/12.
 */
public class HadoopTest {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("d:/demo/core-site.xml"));
        conf.addResource(new Path("d:/demo/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);



        Path path = new Path("hdfs://hadooptest:9000/mark/job/a.txt");
        if (fs.exists(path)) {
            System.out.println("true");
        } else {
            System.out.println("false");
        }
        RemoteIterator<LocatedFileStatus> it = fs.listFiles(new Path("hdfs://hadooptest:9000/"), true);
        while(it.hasNext()){
            LocatedFileStatus lfs = it.next();
            System.out.println(lfs.getPath());
        }
//        fs.copyFromLocalFile(new Path("d:/demo/ip.txt"),new Path("hdfs://hadooptest:9000/mark/job"));
        fs.copyToLocalFile(new Path("hdfs://hadooptest:9000/mark/job/a.txt"),new Path("d:/doc"));
    }
}
