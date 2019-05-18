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
        if (args.length == 0) {       //绗竴涓弬鏁版槸璇ar鎵�娇鐢ㄧ殑绫伙紝绗簩涓弬鏁版槸鏁版嵁闆嗘墍瀛樻斁鐨勮矾寰�            throw new Exception("You must set input path!");
        }
 
        String fileName = args[args.length-1];  //杈撳叆鐨勬枃浠惰矾寰勬槸鏈�悗涓�釜鍙傛暟
        HBaseImportTest test = new HBaseImportTest();
        test.importLocalFileToHBase(fileName);
    }
 
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
                if (count % 10000 == 0)
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
                table.close(); // must close the client
            } catch (IOException e) {
                e.printStackTrace();
            }
 
        }
        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + " ms");
    }
 
    @SuppressWarnings("deprecation")
    public void put(String line) throws IOException {
        String[] arr = line.split("\t", -1);
        String[] column = {"T_id","T_name","T_length","T_depth","T_height","T_carline","T_country","T_city","T_con_dept","T_con_meth","T_soil","T_fintime","T_lifetime","T_ill","T_whyill","T_cq_design","T_cq_crash_name","T_cq_crash_why","T_cq_crash_ana","T_cq_crash_why_ana","T_cq_crash_dealway"};
 
        if (arr.length == 21) {
            Put put = new Put(Bytes.toBytes(arr[0]));// rowkey
            for(int i=1;i<arr.length;i++){
                put.add(Bytes.toBytes("f1"), Bytes.toBytes(column[i]),Bytes.toBytes(arr[i]));
            }
            table.put(put); // put to server
        }
    }
 
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
                        + kv.getTimestamp()); // mid + time
            }
 
        }
        rsScanner.close();
 
        long en2 = System.currentTimeMillis();
        System.out.println("Total Time: " + (en2 - st) + " ms");
    }
 
}
