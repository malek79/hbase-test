package com.microsoft.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseCompleteExample {

	private static Configuration conf = null;
	/**
	 * Initialization
	 */
	static {
		conf = HBaseConfiguration.create();
		//Define and set the host and the port
		conf.set("hbase.master","malek-pc:16000");
		//Set the configuration: force the configuration
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");

	}

	/**
	 * Create a table
	 **/
	public static void creatTable(String tableName, String[] familys)
			throws Exception {

		//create a connection using createConnection()
		Connection conn = ConnectionFactory.createConnection(conf);
		
		//Get an administrator
		Admin admin = conn.getAdmin();
		
		if (admin.tableExists(TableName.valueOf(tableName))) {
			System.out.println("table already exists!");
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
			for (int i = 0; i < familys.length; i++) {
				tableDesc.addFamily(new HColumnDescriptor(familys[i]));
			}
			admin.createTable(tableDesc);
			System.out.println("create table " + tableName + " ok.");
		}
		
	}

	/**
	 * Delete a table
	 */
	public static void deleteTable(String tableName) throws Exception {

		//create a connection using createConnection()
			Connection conn = ConnectionFactory.createConnection(conf);
			
			//Get an administrator
			Admin admin = conn.getAdmin();
			
			admin.disableTable(TableName.valueOf(tableName));
			admin.deleteTable(TableName.valueOf(tableName));
			
			System.out.println("delete table " + tableName + " ok.");

	}

	/**
	 * Put (or insert) a row
	 **/
	public static void addRecord(String tableName, String rowKey,
			String family, String qualifier, String value) throws Exception {
		try {
			//create a connection using createConnection()
			Connection conn = ConnectionFactory.createConnection(conf);
			
			//Get an administrator
			//Admin admin = conn.getAdmin();

			Table table = conn.getTable(TableName.valueOf(tableName));
			
			Put put = new Put(Bytes.toBytes(rowKey));
			
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes
					.toBytes(value));
			
			table.put(put);
			table.close();
			System.out.println("insert recored " + rowKey + " to table "
					+ tableName + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Delete a row
	 **/
	public static void delRecord(String tableName, String rowKey)
			throws IOException {
		
		//create a connection using createConnection()
		Connection conn = ConnectionFactory.createConnection(conf);
		Table table = conn.getTable(TableName.valueOf(tableName));
		
		List<Delete> list = new ArrayList<Delete>();
		Delete del = new Delete(rowKey.getBytes());
		list.add(del);
		table.delete(list);
		System.out.println("del recored " + rowKey + " ok.");
	}

	/**
	 * Get a row
	 **/
	public static void getOneRecord (String tableName, String rowKey) throws IOException{
		
		//create a connection using createConnection()
		Connection conn = ConnectionFactory.createConnection(conf);
		
		Table table = conn.getTable(TableName.valueOf(tableName));

		Get get = new Get(rowKey.getBytes());
		
		Result rs = table.get(get);
		
		for(Cell kv : rs.listCells()){
			System.out.print(new String(kv.getRowArray()) + " " );
			System.out.print(new String(kv.getFamilyArray()) + ":" );
			System.out.print(new String(kv.getQualifierArray()) + " " );
			System.out.print(kv.getTimestamp() + " " );
			System.out.println(new String(kv.getValueArray()));
		}
	}
	/**
	 * Scan (or list) a table
	 */
	public static void getAllRecord (String tableName) {
		try{
			//create a connection using createConnection()
			Connection conn = ConnectionFactory.createConnection(conf);
			
			Table table = conn.getTable(TableName.valueOf(tableName));
			
			Scan s = new Scan();
			ResultScanner ss = table.getScanner(s);
			
			for(Result r:ss){
				for(Cell kv : r.listCells()){
					System.out.print(new String(kv.getRowArray()) + " ");
					System.out.print(new String(kv.getFamilyArray()) + ":");
					System.out.print(new String(kv.getQualifierArray()) + " ");
					System.out.print(kv.getTimestamp() + " ");
					System.out.println(new String(kv.getValueArray()));
				}
			}
		} catch (IOException e){
			e.printStackTrace();
		}
	}

	public static void main(String[] agrs) {
		try {
			String tablename = "records1";
			String[] familys = { "grade", "course" };
			HBaseCompleteExample.creatTable(tablename, familys);

			// add record zkb
			HBaseCompleteExample.addRecord(tablename, "zkb", "grade", "", "5");
			HBaseCompleteExample.addRecord(tablename, "zkb", "course", "", "90");
			HBaseCompleteExample.addRecord(tablename, "zkb", "course", "math", "97");
			HBaseCompleteExample.addRecord(tablename, "zkb", "course", "art", "87");
			// add record baoniu
			HBaseCompleteExample.addRecord(tablename, "baoniu", "grade", "", "4");
			HBaseCompleteExample.addRecord(tablename, "baoniu", "course", "math", "89");

//			System.out.println("===========get one record========");
//			HBaseCompleteExample.getOneRecord(tablename, "zkb");

			System.out.println("===========show all record========");
			HBaseCompleteExample.getAllRecord(tablename);

//			System.out.println("===========del one record========");
//			HBaseCompleteExample.delRecord(tablename, "baoniu");
//			HBaseCompleteExample.getAllRecord(tablename);
//
//			System.out.println("===========show all record========");
//			HBaseCompleteExample.getAllRecord(tablename);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}