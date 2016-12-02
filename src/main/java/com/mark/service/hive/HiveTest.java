package com.mark.service.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Created by lulei on 2016/12/1.
 */
public class HiveTest {
    private static String driverName =
            "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
        Class.forName(driverName);
        Connection con = DriverManager.getConnection(
                "jdbc:hive2://hadooptest:10000/default", "root", "root");
        Statement stmt = con.createStatement();
        String tableName = "marktb";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName +
                " (key int, value string)");
        System.out.println("Create table success!");
        // show tables
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        // describe table
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }


        sql = "select * from " + tableName;
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(String.valueOf(res.getInt(1)) + "\t"
                    + res.getString(2));
        }

        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

    }
}
