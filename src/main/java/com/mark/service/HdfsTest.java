package com.mark.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by lulei on 2016/11/12.
 */
public class HdfsTest {

    public static void createFile() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("d:/demo/core-site.xml");
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path("/mark/job/to.txt");

        FSDataOutputStream outputStream =  fs.create(dstPath);
        outputStream.write("test".getBytes());
        outputStream.close();
        fs.close();
        System.out.println("create success!");
    }

    public static void main(String[] args) throws IOException {
        HdfsTest.createFile();
    }
}
