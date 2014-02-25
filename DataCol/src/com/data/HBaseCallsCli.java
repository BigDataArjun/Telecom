package com.data;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class HBaseCallsCli extends Configured implements Tool{

		static final byte [] INFO_COLUMNFAMILY = Bytes.toBytes("info");
		static final byte [] ID_QUALIFIER = Bytes.toBytes("global_cell_id");
		static final byte [] E_LON_QUALIFIER = Bytes.toBytes("e_lon");
		static final byte [] E_LAT_QUALIFIER = Bytes.toBytes("e_lat");
		static final byte [] RSRP_OLD_QUALIFIER = Bytes.toBytes("RSRP_bin_avg_dBm_old");
		static final byte [] RSRQ_OLD_QUALIFIER = Bytes.toBytes("RSRQ_bin_avg_dB_old");
		static final byte [] SAMPLES_QUALIFIER = Bytes.toBytes("no_of_sample");
		static final byte [] DATE_QUALIFIER = Bytes.toBytes("date");
		
		  public Map<String, String> getCallsInfo(HTable table, String cell_Id)
			      throws IOException {
			    Get get = new Get(Bytes.toBytes(cell_Id));
			    get.addFamily(INFO_COLUMNFAMILY);
			    Result res = table.get(get);
			    if (res == null) {
			      return null;
			    }
			    Map<String, String> resultMap = new HashMap<String, String>();
			    resultMap.put("global_cell_id", getValue(res, INFO_COLUMNFAMILY, ID_QUALIFIER));
			    resultMap.put("e_lon", getValue(res, INFO_COLUMNFAMILY, E_LON_QUALIFIER));
			    resultMap.put("e_lat", getValue(res, INFO_COLUMNFAMILY, E_LAT_QUALIFIER));
			    resultMap.put("RSRP_bin_avg_dBm_old", getValue(res, INFO_COLUMNFAMILY, RSRP_OLD_QUALIFIER));
			    resultMap.put("RSRQ_bin_avg_dB_old", getValue(res, INFO_COLUMNFAMILY, RSRQ_OLD_QUALIFIER));
			    resultMap.put("no_of_sample", getValue(res, INFO_COLUMNFAMILY, SAMPLES_QUALIFIER));
			    resultMap.put("date", getValue(res, INFO_COLUMNFAMILY, DATE_QUALIFIER));			    
			    return resultMap;
			  }
		  
		  private static String getValue(Result res, byte [] cf, byte [] qualifier) {
			    byte [] value = res.getValue(cf, qualifier);
			    return value == null? "": Bytes.toString(value);
			  }
		
		@Override
	 public int run(String[] args) throws IOException {
	    if (args.length != 1) {
	      System.err.println("Usage: HBaseCallsCli <calls_id>");
	      return -1;
	    }

	    HTable table = new HTable(new HBaseConfiguration(getConf()), "calls");
	    Map<String, String> callsInfo = getCallsInfo(table, args[0]);
	    if (callsInfo == null) {
	      System.err.printf("Station ID %s not found.\n", args[0]);
	      return -1;
	    }
	    for (Map.Entry<String, String> calls : callsInfo.entrySet()) {
	      // Print the date, time, and temperature
	      System.out.printf("%s\t%s\n", calls.getKey(), calls.getValue());
	    }
	    return 0;
	  }

		public static void main(String[] args) throws Exception {
		    int exitCode = ToolRunner.run(new HBaseConfiguration(),
		        new HBaseCallsCli(), args);
		    System.exit(exitCode);
		  }	

}
