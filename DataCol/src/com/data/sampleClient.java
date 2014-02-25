package com.data;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class sampleClient {
	
	public static void main(String[] args) throws IOException, java.io.IOException{
		Configuration config = HBaseConfiguration.create();
		HBaseAdmin admin = new HBaseAdmin(config);
		HTableDescriptor htd = new HTableDescriptor("test_java");
		HColumnDescriptor hcd = new HColumnDescriptor("data");
		htd.addFamily(hcd);
		//admin.createTable(htd);
		byte[] tablename = htd.getName();
		HTableDescriptor[] tables = admin.listTables();
		if(tables.length !=1 && Bytes.equals(tablename, tables[0].getName())){
			throw new IOException();
		
		}
		HTable table = new HTable(config, tablename);
		byte[] row = Bytes.toBytes("row2");
		Put p1 = new Put(row);
		byte[] databytes = Bytes.toBytes("data");
		p1.add(databytes, Bytes.toBytes("4"), Bytes.toBytes("value4"));
		table.put(p1);
		System.out.println("--------> After Put(p1)");
		Get g = new Get(Bytes.toBytes("row3"));
		Result result = table.get(g);
		
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		System.out.println("Scanner: "+scanner);
		try{
			for(Result scannerResult : scanner){
				System.out.println("ScannerResult: "+ scannerResult);
				if(Bytes.toString(scannerResult.getRow()).equals(Bytes.toString(result.getRow()))){
				//if(Bytes.toString(scannerResult.getRow())==Bytes.toString(result.getRow())){
					System.out.println("I am in Row2: "+Bytes.toString(scannerResult.getRow()));
				}
				if(Bytes.toString(scannerResult.getRow()).equals("row1")){
					System.out.println("In Row1: "+Bytes.toString(scannerResult.getRow()));
				}
			}
		}finally{
			scanner.close();
		}
		
		System.out.println("GET: "+ result);
		System.out.println("getMap() "+ result.getMap());
		System.out.println("getColumn() "+ result.getColumn(databytes,Bytes.toBytes("1")));
		System.out.println("getRow() "+ Bytes.toString(result.getRow()));
		System.out.println("getValue() "+ Bytes.toString(result.getValue(databytes,Bytes.toBytes("1"))));
		System.out.println("N2 "+ Bytes.toString(result.getColumnLatest(databytes, Bytes.toBytes("3")).getValue()));
		
	}
	
	

}
