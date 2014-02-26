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

public class HBaseCliTeleCom {
	
	public HBaseCliTeleCom() throws IOException{
		
	}
	
	String columnFamily = "info";
	String columnName1 = "global_cell_id";
	String columnName2 = "e_lon";
	String columnName3 = "e_lat";
	String columnName4 = "RSRP_bin_avg_dBm_old";
	String columnName5 = "RSRQ_bin_avg_dB_old";
	String columnName6 = "no_of_sample";
	String columnName7 = "Date";
	
	
	Configuration config = HBaseConfiguration.create();		//Hbase Configuration object		
	HBaseAdmin admin = new HBaseAdmin(config);				//HBase Admin object can be used to drop the tables etc.
	HTableDescriptor htd = new HTableDescriptor();
	HColumnDescriptor hcd = new HColumnDescriptor();
	
	HTable table = new HTable(config, "calls");				// table Calls should be created in Hbase
	Scan scan = new Scan();
	ResultScanner scanner = table.getScanner(scan);
	
	public void HBasePut (byte[] rowKey,String cn1,double cn2,double cn3,double cn4,double cn5,int cn6,String cn7){
		
		Put p1 = new Put(rowKey);								//to write the data to Hbase row
		byte[] dataBytes = Bytes.toBytes(columnFamily);				//info is the column Family
		p1.add(dataBytes, Bytes.toBytes(columnName1), Bytes.toBytes(cn1));	//global_cell_id is the column qualifier followed by the value it self
		p1.add(dataBytes, Bytes.toBytes(columnName2), Bytes.toBytes(cn2));
		p1.add(dataBytes, Bytes.toBytes(columnName3), Bytes.toBytes(cn3));
		p1.add(dataBytes, Bytes.toBytes(columnName4), Bytes.toBytes(cn4));
		p1.add(dataBytes, Bytes.toBytes(columnName5), Bytes.toBytes(cn5));
		p1.add(dataBytes, Bytes.toBytes(columnName6), Bytes.toBytes(cn6));
		p1.add(dataBytes, Bytes.toBytes(columnName7), Bytes.toBytes(cn7));
		try {
			table.put(p1);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();	
	} finally {
	scanner.close();										// writing to the table in Hbase
	}								
	
	}


}

