package com.microsoft.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBASETEST {
	
	public static void main(String[] args) throws IOException {
		System.out.println("start");
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "malek-pc:16000");
//		// Set the configuration: force the configuration
		conf.set("zookeeper.znode.parent", "/hbase-unsecure");
		// Instantiating HbaseAdmin class

		Connection conn = ConnectionFactory.createConnection(conf);
		Admin admin = conn.getAdmin();
		/***** creation table hbase ******/

		// Instantiating table descriptor class
		HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("employee8"));
		// Adding column families to table descriptor
		tableDescriptor.addFamily(new HColumnDescriptor("str1"));
		tableDescriptor.addFamily(new HColumnDescriptor("professional"));
		// Execute the table through admin
		admin.createTable(tableDescriptor);
		System.out.println(" Table created ");

		Table table = conn.getTable(TableName.valueOf("employee8"));
		Put p = new Put(Bytes.toBytes("youssef"));
		p.addColumn(Bytes.toBytes("str1"), Bytes.toBytes("identifiant"), Bytes.toBytes("youssef"));

		table.put(p);
		System.out.println(table.getName());
		table.close();

	}

}
