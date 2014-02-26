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

		HBaseCliTeleCom hbaseCli = new HBaseCliTeleCom();   			//Object of HBaseCliTeleCom class is created

		//		System.out.println("----------------> HBASE CLIENT INITIALIZED");

		byte[] rowKey = Bytes.toBytes(global_cell_id+e_lon+e_lat);
		Scan scan = new Scan();
		ResultScanner scanner = hbaseCli.table.getScanner(scan);
		//System.out.println("scannerResult.size() "+ scannerResult.size());
		//		System.out.println("-----> Scan.numFamilies() "+scan.numFamilies());
		//		System.out.println("-----> Scan.equals() "+scan.equals(rowKey));

		if(!hbaseCli.scan.equals(null)){

			Get g = new Get(rowKey);				//creating an object to read the data from Hbase

			if(!hbaseCli.table.get(g).isEmpty()){			//checking if the rowKey exists
				Result result = hbaseCli.table.get(g);		//if exists then read the record to result object
				//			System.out.println("--------> HBASE SCANNER");
				//			System.out.println("------> getRow()"+ result.getRow()+"-------> rowKey "+ rowKey);

				RSRP_bin_avg_dBm_old = RSRP_bin_avg_dBm_new;
				RSRQ_bin_avg_dB_old = RSRQ_bin_avg_dB_new;
				//				System.out.println("--------> NEW = OLD");

				//As the row exists we have to update it, the following the logic foe RSRP/RSRQ_update

				double N_update = Math.max(N_old+no_of_sample,10^6);
				N_old=Bytes.toInt(result.getColumnLatest(Bytes.toBytes("info"), Bytes.toBytes("no_of_sample")).getValue());
				RSRP_bin_avg_linear_update = (N_old*Math.pow(10,RSRP_bin_avg_dBm_old/10) + no_of_sample * Math.pow(10,RSRP_bin_avg_dBm_new/10))/no_of_sample+N_old;
				RSRQ_bin_avg_linear_update = (N_old*Math.pow(10,RSRQ_bin_avg_dB_old/10) + no_of_sample * Math.pow(10,RSRQ_bin_avg_dB_new/10))/no_of_sample+N_old;
				RSRP_bin_avg_dBm_update = Math.log10(RSRP_bin_avg_linear_update);
				RSRQ_bin_avg_dB_update = Math.log10(RSRQ_bin_avg_linear_update);

				hbaseCli.HBasePut(rowKey, global_cell_id, e_lon, e_lat, RSRP_bin_avg_dBm_update, RSRQ_bin_avg_dB_update, no_of_sample, Date);
				System.out.println("-------> IN UPDATE LOOP");
			}	

			else
			{	
				//Insert Operation

				hbaseCli.HBasePut(rowKey, global_cell_id, e_lon, e_lat, RSRP_bin_avg_dBm_new, RSRQ_bin_avg_dB_new, no_of_sample, Date);
				System.out.println("-------> IN HBASE INSERT LOOP");
			}
		}
		output.collect(NullWritable.get(), new Text(global_cell_id+","+e_lon+","+e_lat+","+RSRP_bin_avg_dBm_new+","+RSRQ_bin_avg_dB_new +","+no_of_sample+","+Date));
		System.out.println("------> AFTER OUTPUT COLLECTOR");		
	}
}
