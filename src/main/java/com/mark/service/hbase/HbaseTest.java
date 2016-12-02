package com.mark.service.hbase;

import com.google.protobuf.ServiceException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * Created by lulei on 2016/11/22.
 */
public class HbaseTest {

    public static void createTable(String tableName) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", "2181");
        configuration.set("hbase.zookeeper.quorum", "192.168.154.96");
        configuration.set("hbase.master", "hadooptest:6000");
        System.out.println("start create table ......");

        HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
        if (hBaseAdmin.tableExists(tableName)) {
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println(tableName + " is exist,detele....");
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("column1"));
        tableDescriptor.addFamily(new HColumnDescriptor("column2"));
        tableDescriptor.addFamily(new HColumnDescriptor("column3"));
        hBaseAdmin.createTable(tableDescriptor);

        System.out.println("end create table ......");
    }

    public static void main(String[] args) throws ServiceException, IOException {
        createTable("mark");

    }
}
