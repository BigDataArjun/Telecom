package com.data;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import com.data.colData;
import com.data.colDataMapper;
import com.data.colDataReducer;

public class colData {
	
	
	public static void main(String[] args) throws IOException, URISyntaxException {
		  	 
	 JobConf conf = new JobConf(colData.class);		//creating a Job configuration object
	 conf.setJobName("colData");
	 //DistributedCache.addCacheFile(new URI("/user/hduser/project1/compData.csv"), conf);

	 FileInputFormat.setInputPaths(conf, new Path("/user/hduser/project1/Input/sample_call_data_wh.csv")); //Input file path, usually this should not be hard coded
	 FileOutputFormat.setOutputPath(conf, new Path("/user/hduser/project1/Output"));
	 
	 Path outputDir = new Path("/user/hduser/project1/Output");	//The Output location 
	 outputDir.getFileSystem(conf).delete(outputDir, true);		//
	 FileSystem fs = FileSystem.get(conf);						//
	 fs.delete(outputDir,true);									//Deleting the output location if exists
	 
	 conf.setMapperClass(colDataMapper.class);					//Setting and initiating the Mapper class

	 conf.setReducerClass(colDataReducer.class);				//Setting and initiating the Reducer Class
	// conf.setCombinerClass(colDataReducer.class);

	 conf.setOutputKeyClass(Text.class);						//Output key format
	 conf.setOutputValueClass(Text.class);						//Output value format

	 JobClient.runJob(conf);									//Run the Job
	}

}
