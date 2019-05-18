package com.lyx;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
 
public class HBaseImportTest extends Thread {
    public Configuration config;
    public HTable table;
    public HBaseAdmin admin;
 
    public HBaseImportTest() {
        config = HBaseConfiguration.create();
        try {
            table = new HTable(config, Bytes.toBytes("tunnel"));
            admin = new HBaseAdmin(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
 
    public static void main(String[] args) throws Exception {
        if (args.length == 0) {       //如果未输入参数提示输入数据集地址            
         throw new Exception("You must set input path!");
        }
 
        String fileName = args[args.length-1];  //第二个参数为数据集地址
        HBaseImportTest test = new HBaseImportTest();
        test.importLocalFileToHBase(fileName);
    }
 /**
* 对数据集文件进行处理
* @param fileName 本地数据文件名
*/

    public void importLocalFileToHBase(String fileName) {
        long st = System.currentTimeMillis();
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(
                    fileName)));
            String line = null;
            int count = 0;
            while ((line = br.readLine()) != null) {
                count++;
                put(line);
                if (count % 100000 == 0)//没10万行数据输出上传提示
                    System.out.println(count);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
 
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
 
            try {
                table.flushCommits();
                table.close(); // 关闭Hbase客户端
            } catch (IOException e) {
                e.printStackTrace();
            }
 
        }
        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + " ms");
    }
 
    @SuppressWarnings("deprecation")
 /**
* 将一行由逗号分隔的数据上传至Hbase
* @param line 经过行分割后的一行字符串
*/

    public void put(String line) throws IOException {
        String[] arr = line.split("\t", -1);
        String[] column = {"T_id","T_name","T_length","T_depth","T_height","T_carline","T_country","T_city","T_con_dept","T_con_meth","T_soil","T_fintime","T_lifetime","T_ill","T_whyill","T_cq_design","T_cq_crash_name","T_cq_crash_why","T_cq_crash_ana","T_cq_crash_why_ana","T_cq_crash_dealway"};
 
        if (arr.length == 21) {
            Put put = new Put(Bytes.toBytes(arr[0]));// 行键
            for(int i=1;i<arr.length;i++){
                put.add(Bytes.toBytes("f1"), Bytes.toBytes(column[i]),Bytes.toBytes(arr[i]));
            }
            table.put(put); // put加入表
        }
    }
 /**
	* 获取Hbase数据
	* @param rowkey 行键
	* @param columnFamily列族
	* @param column列
	* @param columnFamily数据库版本
	*/

    public void get(String rowkey, String columnFamily, String column,
            int versions) throws IOException {
        long st = System.currentTimeMillis();
 
        Get get = new Get(Bytes.toBytes(rowkey));
        get.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
 
        Scan scanner = new Scan(get);
        scanner.setMaxVersions(versions);
 
        ResultScanner rsScanner = table.getScanner(scanner);
 
        for (Result result : rsScanner) {
            final List<KeyValue> list = result.list();
            for (final KeyValue kv : list) {
                System.out.println(Bytes.toStringBinary(kv.getValue()) + "\t"
                        + kv.getTimestamp()); //加入时间戳
            }
 
        }
        rsScanner.close();
 
        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + " ms");
    }
 
}
