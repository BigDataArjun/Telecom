package com.data;

import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.DatatypeConverter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.record.CsvRecordInput;

public class colDataMapper extends MapReduceBase
implements Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)	//declaring the input output format of Key/Value pair
					throws IOException {
		// TODO Auto-generated method stub
		String line = value.toString();								//reading the value to a string
		int sample = 0;
		int sample1 = 0;
		double RSRP = 0.0;
		double RSRQ = 0.0;
		int no_of_rows_in_map_table = 0;
		double RSRP_bin_avg_dBm_old = 0.0;
		double RSRQ_bin_avg_dBm_old = 0.0;
		double RSRP_bin_avg_dBm_new = 0.0;
		double RSRQ_bin_avg_dBm_new = 0.0;
		int N_old = 0;
		double RSRP_bin_avg_linear_update = 0.0;
		double RSRQ_bin_avg_linear_update = 0.0;
		double avgRSRP = 0.0;
		double avgRSRQ = 0.0;
		double e_lon = 0.0;
		double e_lat = 0.0;

		String[] dataArray = line.split(",");					// split the input line with , as delimiter
		
		if(!dataArray[29].equals("") && !dataArray[30].equals(""))
		{
			if(!dataArray[19].equals(""))
			{
				int num = Integer.parseInt(dataArray[19]);
				//String hex_nodeb_id = new String(DatatypeConverter.parseHexBinary(num));
				String hex_num = Integer.toHexString(num);			//conversion of decimal to hexadecimal
				String hex_nodeb_id = hex_num.substring(0,5);		//dividing the hexadecimal number into two strings
				String hex_cell_id = hex_num.substring(5, 7);
				int nodeb_id  = Integer.parseInt(hex_nodeb_id, 16);	//converting them back to decimal
				int cell_id = Integer.parseInt(hex_cell_id, 16);
				String global_cell_id = Integer.toString(nodeb_id).concat("-").concat(Integer.toString(cell_id));	//Generating a global_cell_Id by appending a - between the two string
				//System.out.println("-----"+dataArray[19]+"->"+hex_num+"->"+hex_nodeb_id+"->"+hex_cell_id+"->"+nodeb_id+"->"+cell_id+"-----");
				//System.out.println("->"+global_cell_id+"<-");
				if(!dataArray[6].equals("") && !dataArray[7].equals(""))
				{//Formula logic to find the e_longitude & e_latitude 
					double sigmaLat = 1.618;
					e_lat = Math.floor(Double.parseDouble(dataArray[6])/sigmaLat);
					double sigmaLon = sigmaLat/Math.cos(Double.parseDouble(dataArray[6]));
					e_lon = Math.floor(Double.parseDouble(dataArray[7])/sigmaLon);
					//System.out.println(dataArray[6]+"->"+lat+"->"+dataArray[7]+"->"+lon);

				}
				DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
				StringBuilder out = new StringBuilder();
				out.append(global_cell_id+e_lon+e_lat).append(",").append(global_cell_id).append(",").append(e_lon).append(",").append(e_lat).append(",").append(dataArray[29]).append(",").append(dataArray[30]).append(",").append(dateFormat.format(date));
				//String s = Double.toString(e_lon);
				String oLine = out.toString();
				//System.out.println("---------> IN MAPPER");
				output.collect(new Text(global_cell_id+e_lon+e_lat), new Text(oLine));//Output collector will send the data to the Reducer after shuffle and sort phase
			}
		}
	}
}
