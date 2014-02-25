package com.data;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.*;

public class HBaseInsertUpdate extends Configured implements Tool{
	
	
	static class HBaseInsertUpdateMapper<K, V> extends MapReduceBase implements
    Mapper<LongWritable, Text, K, V> {
  //private NcdcRecordParser parser = new NcdcRecordParser();
  private HTable table;

  public void map(LongWritable key, Text value,
    OutputCollector<K, V> output, Reporter reporter) throws IOException {
    System.out.println("----------> IN INSERT MAPPER");
	  String global_cell_id = null;
		double e_lon = 0.0;
		double e_lat = 0.0;
		double RSRP_bin_avg_dBm_new = 0.0;
		double RSRQ_bin_avg_dB_new = 0.0;
		double RSRP_bin_avg_dBm_old = 0.0;
		double RSRQ_bin_avg_dB_old = 0.0;
		int no_of_sample = 0;
		String Date = null;
		String line = value.toString();
		String[] dataArray = line.split(",");
		String rowKey = global_cell_id+e_lon+e_lat;
		//RSRP_bin_avg_dBm_old = dataArray[4]; 
		byte[] row = Bytes.toBytes(rowKey);
		Put p1 = new Put(row);
		System.out.println("Put(row)----->"+row);
		byte[] dataBytes = Bytes.toBytes("info");
		p1.add(dataBytes, Bytes.toBytes("global_cell_id"), Bytes.toBytes(dataArray[0]));
		p1.add(dataBytes, Bytes.toBytes("e_lon"), Bytes.toBytes(dataArray[1]));
		p1.add(dataBytes, Bytes.toBytes("e_lat"), Bytes.toBytes(dataArray[2]));
		//p1.add(dataBytes, Bytes.toBytes("global_cell_id"), Bytes.toBytes(global_cell_id));
		p1.add(dataBytes, Bytes.toBytes("RSRP_bin_avg_dBm_new"), Bytes.toBytes(dataArray[3]));
		p1.add(dataBytes, Bytes.toBytes("RSRQ_bin_avg_dB_new"), Bytes.toBytes(dataArray[4]));
		p1.add(dataBytes, Bytes.toBytes("no_of_sample"), Bytes.toBytes(dataArray[5]));
		p1.add(dataBytes, Bytes.toBytes("Date"), Bytes.toBytes(dataArray[6]));
		table.put(p1);
		System.out.println("-------> IN HBASE INSERT LOOP");
		
//	  parser.parse(value.toString());
//    if (parser.isValidTemperature()) {
//      byte[] rowKey = RowKeyConverter.makeObservationRowKey(parser.getStationId(),
//        parser.getObservationDate().getTime());
//      Put p = new Put(rowKey);
//      p.add(HBaseTemperatureCli.DATA_COLUMNFAMILY,
//        HBaseTemperatureCli.AIRTEMP_QUALIFIER,
//        Bytes.toBytes(parser.getAirTemperature()));
//      table.put(p);
//    }
  }

 
@SuppressWarnings("deprecation")
public void configure(JobConf jconf) {
	System.out.println("---------> IN CONFIGURE");
    super.configure(jconf);
    // Create the HBase table client once up-front and keep it around
    // rather than create on each map invocation.
    try {
      this.table = new HTable(new HBaseConfiguration(jconf), "calls");
    } catch (IOException e) {
      throw new RuntimeException("Failed HTable construction", e);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    table.close();
  }
} 
		public int run(String[] args) throws IOException {
//			if (args.length != 1) {
//				System.err.println("Usage: HBaseInsertUpdate <input>");
//		      return -1;
//		    }
			System.out.println("---------> in RUN");
		    JobConf jconf = new JobConf(getConf(), getClass());
		    FileInputFormat.addInputPath(jconf, new Path("/user/hduser/project1/Output/part-00000"));
		    jconf.setMapperClass(HBaseInsertUpdateMapper.class);
		    jconf.setNumReduceTasks(0);
		    jconf.setOutputFormat(NullOutputFormat.class);
		    JobClient.runJob(jconf);
		    System.out.println("---------> OUT OF RUN");
		    return 0;
		    
		  }

		  public static void main(String[] args) throws Exception {
			  System.out.println("---------> in MAIN");
			  @SuppressWarnings("deprecation")
			int exitCode = ToolRunner.run(new HBaseConfiguration(),
		        new HBaseInsertUpdate(), args);
		    System.exit(exitCode);
		  }

}
