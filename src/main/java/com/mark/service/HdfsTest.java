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

    public static  void hdfs2Demo() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://hadooptest:9000");
        FileSystem fileSystem =  FileSystem.get(conf);
//        if(!fileSystem.exists(new Path("/mark/test"))){
//            fileSystem.mkdirs(new Path("/mark/test"));
//        }
//
//        if(fileSystem.exists(new Path("/mark/test"))){
//            fileSystem.deleteOnExit(new Path("/mark/test"));
//        }

//        FSDataOutputStream out = fileSystem.create(new Path("/mark/test/ip.txt"));
//        FileInputStream in = new FileInputStream("d:/demo/ip.txt");
//        IOUtils.copyBytes(in,out,1024,true);

        fileSystem.copyFromLocalFile(new Path("d:/demo/ip.txt"),new Path("/mark/test/ip.txt"));

        fileSystem.rename(new Path("/mark/test/ip.txt"),new Path("/mark/test/ip2.txt"));

//        fileSystem.delete(new Path("/mark/test"),true);


//        FileStatus[]  fs = fileSystem.listStatus(new Path("/"));
//        for(FileStatus f : fs){
//            String dir = f.isDirectory() ?"dir":"file";
//            String name = f.getPath().getName();
//            String path = f.getPath().toString();
//            System.out.println(dir+"----"+name+"  path:"+path);
//            System.out.println(f.getAccessTime());
//            System.out.printf(f.getBlockSize()+"");
//            System.out.println(f.getGroup());
//            System.out.println(f.getLen());
//            System.out.println(f.getModificationTime());
//            System.out.println(f.getOwner());
//            System.out.println(f.getPermission());
//            System.out.println(f.getReplication());
//        }

        FileStatus fs = fileSystem.getFileStatus(new Path("/mark/test/ip2.txt"));
        BlockLocation[] bls = fileSystem.getFileBlockLocations(fs,0,fs.getLen());
        for(int i=0; i<bls.length; i++){
            String[] hosts = bls[i].getHosts();
            System.out.println("block_" + i + "_location: " + hosts[0]);
        }

        DistributedFileSystem hdfs = (DistributedFileSystem) fileSystem;
        DatanodeInfo[] dns = hdfs.getDataNodeStats();
        for(int i=0; i<dns.length; i++){
            System.out.println("datanode_" + i + "_name:" +dns[i].getHostName()) ;
        }
    }


    public static void main(String[] args) throws IOException {
//        createFile();
//        updateFile();
//        renameFile();
        hdfs2Demo();
    }
}
