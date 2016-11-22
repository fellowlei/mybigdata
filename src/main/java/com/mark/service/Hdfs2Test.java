package com.mark.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * Created by lulei on 2016/11/21.
 */
public class Hdfs2Test {

    public static void uploadFile() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadooptest:9000");
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataOutputStream out = fileSystem.create(new Path("/mark/test/input.txt"));
        FileInputStream in = new FileInputStream("d:/demo/input.txt");
        IOUtils.copyBytes(in, out, 1024, true);

    }

    public static void hdfs2Demo() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadooptest:9000");
        FileSystem fileSystem = FileSystem.get(conf);
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

        fileSystem.copyFromLocalFile(new Path("d:/demo/ip.txt"), new Path("/mark/test/ip.txt"));

        fileSystem.rename(new Path("/mark/test/ip.txt"), new Path("/mark/test/ip2.txt"));

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
        BlockLocation[] bls = fileSystem.getFileBlockLocations(fs, 0, fs.getLen());
        for (int i = 0; i < bls.length; i++) {
            String[] hosts = bls[i].getHosts();
            System.out.println("block_" + i + "_location: " + hosts[0]);
        }

        DistributedFileSystem hdfs = (DistributedFileSystem) fileSystem;
        DatanodeInfo[] dns = hdfs.getDataNodeStats();
        for (int i = 0; i < dns.length; i++) {
            System.out.println("datanode_" + i + "_name:" + dns[i].getHostName());
        }
    }

    public static void main(String[] args) throws IOException {
//        hdfs2Demo();
        uploadFile();
    }
}
