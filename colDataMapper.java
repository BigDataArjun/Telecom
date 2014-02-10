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
		
		//System.out.println("=="+line+"==");
		    int sample = 0;
		    int sample1 = 0;
		    //int j = 1;
		    //int k = 1;
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
		    //double[] RSRP = new double[5];
			//double[] RSRQ = new double[5];
			//List RSRPlist = new ArrayList();
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
//		    if(!dataArray[19].equals(""))
//		    {
//		    	int num = Integer.parseInt(dataArray[19]);
//		    	//String hex_nodeb_id = new String(DatatypeConverter.parseHexBinary(num));
//		    	String hex_num = Integer.toHexString(num);
//		    	String hex_nodeb_id = hex_num.substring(0,5);
//		    	String hex_cell_id = hex_num.substring(5, 7);
//		    	int nodeb_id  = Integer.parseInt(hex_nodeb_id, 16);
//		    	int cell_id = Integer.parseInt(hex_cell_id, 16);
//		    	String global_cell_id = Integer.toString(nodeb_id).concat("-").concat(Integer.toString(cell_id));
//		    	//System.out.println("-----"+dataArray[19]+"->"+hex_num+"->"+hex_nodeb_id+"->"+hex_cell_id+"->"+nodeb_id+"->"+cell_id+"-----");
//		    	//System.out.println("->"+global_cell_id+"<-");
//		    if(!dataArray[6].equals("") && !dataArray[7].equals(""))
//		    {
//		    	double sigmaLat = 1.618;
//		    	lat = Math.floor(Double.parseDouble(dataArray[6])/sigmaLat);
//		    	double sigmaLon = sigmaLat/Math.cos(Double.parseDouble(dataArray[6]));
//		    	lon = Math.floor(Double.parseDouble(dataArray[7])/sigmaLon);
//		    	//System.out.println(dataArray[6]+"->"+lat+"->"+dataArray[7]+"->"+lon);
//		    	
//		    }
//		    //System.out.println("*"+dataArray[35]+"*");
//			for(int i=35;i<45;i=i+3){
//			if(!dataArray[i].equals(""))
//			{
//				sample = sample+1;
//				//RSRP[j] = Math.pow(10,(Double.parseDouble(dataArray[i]))/10);
//				RSRP = RSRP + Math.pow(10,(Double.parseDouble(dataArray[i]))/10);
//				//RSRPlist = Math.pow(10,(Double.parseDouble(dataArray[i]))/10);
//			//j=j+1;
//			//System.out.println("*RSRP="+dataArray[i]+"*");
//			}
//			//else {RSRP[j]= 0.0; j=j+1;}
//			}
//			
//			for(int i=36;i<46;i=i+3){
//				if(!dataArray[i].equals(""))
//				{
//					sample1 = sample1+1;
//					//RSRQ[k] = Math.pow(10,(Double.parseDouble(dataArray[i]))/10);
//					RSRQ = RSRQ + Math.pow(10,(Double.parseDouble(dataArray[i]))/10);
//				//k=k+1;
//				//System.out.println("*RSRQ="+dataArray[i]+"*");
//				}
//				//else { RSRQ[k]=0.0; k=k+1;}
//				}
//			//int RSRP_bin_avg_dBm_new; 
//			//double avgRSRP = 10*Math.log10((RSRP_1+RSRP_2+RSRP_3+RSRP_4)/sample);
//			//double avgRSRP = 10*Math.log10((RSRP[1]+RSRP[2]+RSRP[3]+RSRP[4])/sample);
//			//double avgRSRQ = 10*Math.log10((RSRQ[1]+RSRQ[2]+RSRQ[3]+RSRQ[4])/sample1);
//			RSRP_bin_avg_dBm_new = 10*Math.log10((RSRP)/sample);
//			RSRQ_bin_avg_dBm_new = 10*Math.log10((RSRQ)/sample1);
//			
//			if(RSRP_bin_avg_dBm_new>0)
//			{
//				no_of_rows_in_map_table++;
//				RSRP_bin_avg_dBm_old = RSRP_bin_avg_dBm_new;
//				RSRQ_bin_avg_dBm_old = RSRQ_bin_avg_dBm_new;
//			}
//			if(no_of_rows_in_map_table == 1)
//			{
//				N_old=2;
//				RSRP_bin_avg_linear_update = (N_old*Math.pow(10,RSRP_bin_avg_dBm_old/10) + sample * Math.pow(10,RSRP_bin_avg_dBm_new/10))/sample+N_old;
//				RSRQ_bin_avg_linear_update = (N_old*Math.pow(10,RSRQ_bin_avg_dBm_old/10) + sample1 * Math.pow(10,RSRQ_bin_avg_dBm_new/10))/sample1+N_old;
//				avgRSRP = Math.log10(RSRP_bin_avg_linear_update);
//				avgRSRQ = Math.log10(RSRQ_bin_avg_linear_update);
//			}
//			
//			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//			Date date = new Date();
//			StringBuilder out = new StringBuilder();
//			out.append(lon).append(",").append(lat).append(",").append(global_cell_id).append(",").append(RSRP_bin_avg_dBm_new).append(",").append(RSRQ_bin_avg_dBm_new).append(",").append(dateFormat.format(date));
//			
//			String oLine = out.toString();
//			output.collect(NullWritable.get(), new Text(oLine));
//		   }
	}
}
