package com.data;

import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.CsvRecordInput;

public class colDataReducer extends MapReduceBase
implements Reducer<Text, Text, NullWritable, Text> {

	//private Path[] localFiles;
	//private computationData compData;
	
//	public void configure(JobConf conf)
//	{
//	
//		try {
//			localFiles = DistributedCache.getLocalCacheFiles(conf);
//			
//			//CsvRecordInput csv = new CsvRecordInput());
//			
//			
//
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//	}
	
	@Override
	public void reduce(Text global_cell_id_elon_elat, Iterator<Text> values,
			OutputCollector<NullWritable, Text> output, Reporter reporter)
			throws IOException {
		double e_lon=0.0;
		double e_lat=0.0;
		double RSRP=0.0;
		double RSRQ=0.0;
		String Date=null;
		int no_of_sample=0;
		int N_old =0;
		double sumRSRP=0.0;
		double sumRSRQ=0.0;
		double RSRP_bin_avg_dBm_new = 0.0;
		double RSRQ_bin_avg_dB_new = 0.0;
		double RSRQ_bin_avg_dB_old = 0.0;
		double RSRP_bin_avg_dBm_old = 0.0;
		double RSRP_bin_avg_linear_update = 0.0;
		double RSRQ_bin_avg_linear_update = 0.0;
		double RSRP_bin_avg_dBm_update = 0.0;
		double RSRQ_bin_avg_dB_update = 0.0;
		String global_cell_id = null;
		//global_cellid=global_cell_id.toString();
		System.out.println("-------->IN REDUCER");
		while(values.hasNext()){
			String tokens[]= (values.next().toString()).split(",");
			global_cell_id = tokens[1].toString();
			e_lon = Double.parseDouble(tokens[2]);
			e_lat = Double.parseDouble(tokens[3]);
			RSRP = Double.parseDouble(tokens[4]);
			RSRQ = Double.parseDouble(tokens[5]);
			Date = tokens[6];
			no_of_sample++;
			sumRSRP = sumRSRP + Math.pow(10,(RSRP)/10);
			sumRSRQ =sumRSRQ + Math.pow(10,RSRQ/10);
			//System.out.println(RSRP + "+++" + sumRSRP);
			//System.out.println(global_cell_id+","+e_lon+","+e_lat+","+RSRP+","+RSRQ+","+sumRSRP+","+sumRSRQ+","+Date);
		}
		RSRP_bin_avg_dBm_new = 10*Math.log10((sumRSRP)/no_of_sample);	//calculating the average RSRP & RSRQ
		RSRQ_bin_avg_dB_new = 10*Math.log10((sumRSRP)/no_of_sample);
		//System.out.println("-------->"+RSRP + RSRP_bin_avg_dBm_new);
		//System.out.println("-------->"+global_cell_id+","+e_lon+","+e_lat+","+RSRP+","+RSRQ+","+RSRP_bin_avg_dBm_new+","+RSRQ_bin_avg_dB_new+","+Date);
//		FileReader file = new FileReader("/user/hduser/project1/compData.csv");
//		String dtokens[]=Integer.toString(file.read()).split(",");
		//String rowKey = (global_cell_id+e_lon+e_lat);
			
		//HBase Client 
		
		//System.out.println("-------> Before HBASE CONFIG");
		Configuration config = HBaseConfiguration.create();		//Hbase Configuration object		
		HBaseAdmin admin = new HBaseAdmin(config);				//HBase Admin object can be used to drop the tables etc.
		HTableDescriptor htd = new HTableDescriptor();
		HColumnDescriptor hcd = new HColumnDescriptor();
		
		//admin.enableTable("calls");
		HTable table = new HTable(config, "calls");				// table Calls should be created in Hbase
		//Get g = new Get();
		//Get tableCF = new Get(Bytes.toBytes("calls"));
		byte[] rowKey = Bytes.toBytes(global_cell_id+e_lon+e_lat);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		//try{
		
		
		//System.out.println("scannerResult.size() "+ scannerResult.size());
		System.out.println("-----> Scan.numFamilies() "+scan.numFamilies());
		System.out.println("-----> Scan.equals() "+scan.equals(rowKey));
		
		if(!scan.equals(null)){
			
		//try{
		//for(Result scannerResult: scanner){
		Get g = new Get(rowKey);				//creating an object to read the data from Hbase
		
		
		if(!table.get(g).isEmpty()){			//checking if the rowKey exists
			Result result = table.get(g);		//if exists then read the record to result object
			System.out.println("--------> HBASE SCANNER");
			System.out.println("------> getRow()"+ result.getRow()+"-------> rowKey "+ rowKey);
			//if (Bytes.toString(scannerResult.getRow()).equals(Bytes.toString(rowKey))){
				
				RSRP_bin_avg_dBm_old = RSRP_bin_avg_dBm_new;
				RSRQ_bin_avg_dB_old = RSRQ_bin_avg_dB_new;
				System.out.println("--------> NEW = OLD");
				//As the row exists we have to update it, the following the logic foe RSRP/RSRQ_update
					double N_update = Math.max(N_old+no_of_sample,10^6);
					N_old=Bytes.toInt(result.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("no_of_sample")).getValue());
					RSRP_bin_avg_linear_update = (N_old*Math.pow(10,RSRP_bin_avg_dBm_old/10) + no_of_sample * Math.pow(10,RSRP_bin_avg_dBm_new/10))/no_of_sample+N_old;
					RSRQ_bin_avg_linear_update = (N_old*Math.pow(10,RSRQ_bin_avg_dB_old/10) + no_of_sample * Math.pow(10,RSRQ_bin_avg_dB_new/10))/no_of_sample+N_old;
					RSRP_bin_avg_dBm_update = Math.log10(RSRP_bin_avg_linear_update);
					RSRQ_bin_avg_dB_update = Math.log10(RSRQ_bin_avg_linear_update);
					Put p1 = new Put(rowKey);								//to write the data to Hbase row
					byte[] dataBytes = Bytes.toBytes("info");				//info is the column Family
					p1.add(dataBytes, Bytes.toBytes("global_cell_id"), Bytes.toBytes(global_cell_id));	//global_cell_id is the column qualifier followed by the value it self
					p1.add(dataBytes, Bytes.toBytes("e_lon"), Bytes.toBytes(e_lon));
					p1.add(dataBytes, Bytes.toBytes("e_lat"), Bytes.toBytes(e_lat));
					//p1.add(dataBytes, Bytes.toBytes("global_cell_id"), Bytes.toBytes(global_cell_id));
					p1.add(dataBytes, Bytes.toBytes("RSRP_bin_avg_dBm_old"), Bytes.toBytes(RSRP_bin_avg_dBm_update));
					p1.add(dataBytes, Bytes.toBytes("RSRQ_bin_avg_dB_old"), Bytes.toBytes(RSRQ_bin_avg_dB_update));
					p1.add(dataBytes, Bytes.toBytes("no_of_sample"), Bytes.toBytes(no_of_sample));
					p1.add(dataBytes, Bytes.toBytes("Date"), Bytes.toBytes(Date));
					table.put(p1);											// writing to the table in Hbase
					System.out.println("-------> IN UPDATE LOOP");
			}	
			//}
				else
				{	
					//Insert Operation
					
					//byte[] row = Bytes.toBytes(rowKey);
					Put p1 = new Put(rowKey);
					byte[] dataBytes = Bytes.toBytes("info");
					p1.add(dataBytes, Bytes.toBytes("global_cell_id"), Bytes.toBytes(global_cell_id));
					p1.add(dataBytes, Bytes.toBytes("e_lon"), Bytes.toBytes(e_lon));
					p1.add(dataBytes, Bytes.toBytes("e_lat"), Bytes.toBytes(e_lat));
					//p1.add(dataBytes, Bytes.toBytes("global_cell_id"), Bytes.toBytes(global_cell_id));
					p1.add(dataBytes, Bytes.toBytes("RSRP_bin_avg_dBm_old"), Bytes.toBytes(RSRP_bin_avg_dBm_new));
					p1.add(dataBytes, Bytes.toBytes("RSRQ_bin_avg_dB_old"), Bytes.toBytes(RSRQ_bin_avg_dB_new));
					p1.add(dataBytes, Bytes.toBytes("no_of_sample"), Bytes.toBytes(no_of_sample));
					p1.add(dataBytes, Bytes.toBytes("Date"), Bytes.toBytes(Date));
					table.put(p1);
					System.out.println("-------> IN HBASE INSERT LOOP");
				}
			}
//		}
//		} finally {
//			scanner.close();
//	}
		//admin.disableTable("calls");
		//System.out.println("----------->"+localFiles.toString()+",");
		//output.collect(new Text(global_cell_id_elon_elat), new Text(global_cell_id+","+e_lon+","+e_lat+","+RSRP_bin_avg_dBm_new+","+RSRQ_bin_avg_dB_new +","+Date));
		output.collect(NullWritable.get(), new Text(global_cell_id+","+e_lon+","+e_lat+","+RSRP_bin_avg_dBm_new+","+RSRQ_bin_avg_dB_new +","+no_of_sample+","+Date));
		//output.collect(NullWritable.get(), NullWritable.get());
		System.out.println("------> AFTER OUTPUT COLLECTOR");
//		String line = values.toString();
//		if(global_cell_id.equals(global_cell_id))
//		{
//			//values.
//			//line = values.toString();
//			//String[] dataArray = line.split(",");
//			System.out.println("---->"+line+""+global_cell_id);
//			
//		}
		
//		output.collect(new Text(global_cell_id), new Text(e_lon+","+e_lat+","+RSRP+","+RSRQ));
		
	}

}
