package com.mark.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by lulei on 2016/11/12.
 */
public class HdfsTest {

    public static void createFile() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("d:/demo/core-site.xml");
        conf.addResource(new Path("d:/demo/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path("/mark/job/to.txt");

        FSDataOutputStream outputStream = fs.create(dstPath);
        outputStream.write("test2".getBytes());
        outputStream.close();
        fs.close();
        System.out.println("create success!");
    }

    public static void updateFile() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("d:/demo/core-site.xml");
        conf.addResource(new Path("d:/demo/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Path srcPath = new Path("d:/demo/ip.txt");
        Path dstPath = new Path("hdfs://hadooptest/mark/job/ip.txt");
        fs.copyFromLocalFile(false, srcPath, dstPath);

        System.out.println("Upload to " + conf.get("fs.defaultFS"));
        System.out.println("------------list files------------" + "\n");

        FileStatus[] fileStatuses = fs.listStatus(dstPath);
        for (FileStatus file : fileStatuses) {
            System.out.println(file.getPath());
        }
        fs.close();
    }

    public static void renameFile() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("d:/demo/core-site.xml");
        conf.addResource(new Path("d:/demo/hdfs-site.xml"));
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path("hdfs://hadooptest/a/old.txt");
        Path newPath = new Path("hdfs://hadooptest/a/new.txt");
        boolean isok = fs.rename(oldPath, newPath);
        if (isok) {
            System.out.println("rename ok!");
        } else {
            System.out.println("rename failure");
        }
        fs.close();
    }




    public static void main(String[] args) throws IOException {
//        createFile();
//        updateFile();
        renameFile();
    }
}
