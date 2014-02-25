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

public class HBaseInsert extends Configured implements Tool{
	
	
	static class HBaseInsertMapper<K, V> extends MapReduceBase implements Mapper<LongWritable, Text, K, V>{
		
		private HTable table;
		@Override
		public void map(LongWritable key, Text values,
				OutputCollector<K, V> outpur, Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			String global_cell_id = null;
			double e_lon = 0.0;
			double e_lat = 0.0;
			double RSRP_bin_avg_dBm_new = 0.0;
			double RSRQ_bin_avg_dB_new = 0.0;
			int no_of_sample = 0;
			String Date = null;
			String line = values.toString();
			String[] dataArray = line.split(",");
		//Get get = new Get()	
			
		}
		
	}

	@Override
	public int run(String[] args) throws IOException {
	    if (args.length != 1) {
	      System.err.println("Usage: HBaseTemperatureImporter <input>");
	      return -1;
	    }
	    JobConf jconf = new JobConf(getConf(), getClass());
	    FileInputFormat.addInputPath(jconf, new Path("/user/hduser/project1/Output/part-00000"));
	    jconf.setMapperClass(HBaseInsertMapper.class);
	    jconf.setNumReduceTasks(0);
	    jconf.setOutputFormat(NullOutputFormat.class);
	    JobClient.runJob(jconf);
	    return 0;
	  }
	
	public static void main(String[] args) throws Exception {
	    int exitCode = ToolRunner.run(new HBaseConfiguration(),
	        new HBaseInsert(), args);
	    System.exit(exitCode);
	  }

}
