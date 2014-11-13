package main.java.metric;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import umontreal.iro.lecuyer.simevents.Sim;
import main.java.cluster.Cluster;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.entry.Global;
import main.java.utils.Utility;
import main.java.workload.WorkloadBatch;

public class Metric implements java.io.Serializable {
	
	private static final long serialVersionUID = 2422722688394703455L;
	
	public static ArrayList<Double> mean_throughput;
	public static ArrayList<Double> mean_response_time;
	
	public static ArrayList<Double> mean_trFreq;
	public static ArrayList<Double> mean_dti;
	public static ArrayList<Double> percentage_dt;
	public static ArrayList<Double> percentage_ndt;
	public static ArrayList<Double> percentageChangeInDt;
	
	public static ArrayList<Long> total_data;
	
	public static ArrayList<Double> mean_server_inflow;
	public static ArrayList<Double> mean_server_outflow;
	public static ArrayList<Double> mean_server_data;
	public static ArrayList<Double> sd_server_data;
	
	public static ArrayList<Double> mean_partition_inflow;
	public static ArrayList<Double> mean_partition_outflow;
	public static ArrayList<Double> mean_partition_data;
	public static ArrayList<Double> sd_partition_data;
	
	public static ArrayList<Integer> inter_server_dmv;
	public static ArrayList<Integer> intra_server_dmv;
	
	private static File file;
	
	public static void init() {
		mean_throughput = new ArrayList<Double>();
		mean_response_time = new ArrayList<Double>();
		
		mean_trFreq = new ArrayList<Double>();
		mean_dti = new ArrayList<Double>();
		percentage_dt = new ArrayList<Double>();
		percentage_ndt = new ArrayList<Double>();
		percentageChangeInDt = new ArrayList<Double>();
		
		total_data = new ArrayList<Long>();
		
		mean_server_inflow = new ArrayList<Double>();
		mean_server_outflow = new ArrayList<Double>();
		mean_server_data = new ArrayList<Double>();
		sd_server_data = new ArrayList<Double>();
		
		mean_partition_inflow = new ArrayList<Double>();
		mean_partition_outflow = new ArrayList<Double>();
		mean_partition_data = new ArrayList<Double>();
		sd_partition_data = new ArrayList<Double>();
		
		inter_server_dmv = new ArrayList<Integer>();
		intra_server_dmv = new ArrayList<Integer>();
		
		// Creating a metric file
		file = new File(Global.metric_dir+"run"+Global.repeated_runs+"/"
				+Global.simulation+"-s"+Global.servers+"-p"+Global.partitions+".out");
		
		try {			
			file.getParentFile().mkdirs();
			file.createNewFile();
		} catch (IOException e) {
			Global.LOGGER.error("Failed in creating metric directory or file !!", e);
		}
	}
	
	public static void collect(Cluster cluster, WorkloadBatch wb) {
				
		mean_throughput.add(wb.get_throughput());				
		mean_response_time.add(wb.get_response_time());
	
		mean_trFreq.add(wb.get_mean_trFreq());
		mean_dti.add(wb.get_mean_dti());
		percentage_dt.add(wb.get_percentage_dt());
		percentage_ndt.add(wb.get_percentage_ndt());
		percentageChangeInDt.add(wb.get_percentage_change_in_dt());
		
		intra_server_dmv.add(wb.get_intra_dmv());
		inter_server_dmv.add(wb.get_inter_dmv());

		getServerStatistic(cluster);
		getPartitionStatistic(cluster);
	}
	
	public static void getServerStatistic(Cluster cluster) {		
		DescriptiveStatistics _data = new DescriptiveStatistics();
		DescriptiveStatistics _inflow = new DescriptiveStatistics();
		DescriptiveStatistics _outflow = new DescriptiveStatistics();
		
		for(Server server : cluster.getServers()) {
			
			_data.addValue(server.getServer_total_data());
			_inflow.addValue(server.getServer_inflow());
			_outflow.addValue(server.getServer_outflow());				
		}
		
		mean_server_inflow.add(Math.round((_inflow.getMean() * 100.0)) / 100.0);
		mean_server_outflow.add(Math.round((_outflow.getMean() * 100.0)) / 100.0);
		mean_server_data.add(Math.round((_data.getMean() * 100.0)) / 100.0);
		sd_server_data.add(Math.round((_data.getStandardDeviation() * 100.0)) / 100.0);
		total_data.add((long) Global.global_dataCount);
	}
	
	public static void getPartitionStatistic(Cluster cluster) {		
		DescriptiveStatistics _data = new DescriptiveStatistics();
		DescriptiveStatistics _inflow = new DescriptiveStatistics();
		DescriptiveStatistics _outflow = new DescriptiveStatistics();
		
		for(Partition partition : cluster.getPartitions()) {
			
			_data.addValue(partition.getPartition_dataSet().size());
			_inflow.addValue(partition.getPartition_inflow());
			_outflow.addValue(partition.getPartition_outflow());			
		}
		
		mean_partition_inflow.add(Math.round((_inflow.getMean() * 100.0)) / 100.0);
		mean_partition_outflow.add(Math.round((_outflow.getMean() * 100.0)) / 100.0);
		mean_partition_data.add(Math.round((_data.getMean() * 100.0)) / 100.0);
		sd_partition_data.add(Math.round((_data.getStandardDeviation() * 100.0)) / 100.0);		
	}
	
	public static void report() {
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");		
		Global.LOGGER.info("Reporting simulation statistic ...");
		Global.LOGGER.info("Simulation time: "+Sim.time()/3600+" hrs");
		
		if(Global.incrementalRepartitioning)
			Global.LOGGER.info("Incremental repartitioning cycle: "+Global.repartitioningCycle);
		
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Total transactions processed: "+Global.total_transactions);
		Global.LOGGER.info("Total Unique transactions processed: "+Global.global_trSeq);
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Average throughput: "+mean_throughput+" TPS");
		Global.LOGGER.info("Average response time: "+mean_response_time+" ms");
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Percentage change in distributed transactions: "+percentageChangeInDt+" %");
		Global.LOGGER.info("Percentage of distributed transactions: "+percentage_dt+" %");
		Global.LOGGER.info("Percentage of non-distributed transactions: "+percentage_ndt+" %");
		Global.LOGGER.info("Average impact of distributed transactions: "+mean_dti);
		Global.LOGGER.info("Average transactional frequency: "+mean_trFreq);
//		Global.LOGGER.info("_____________________________________________________________________________");
//		Global.LOGGER.info("Average Partition's data inflow: "+mean_partition_inflow);
//		Global.LOGGER.info("Average Partition's data outflow: "+mean_partition_outflow);
//		Global.LOGGER.info("Average Partition's data count: "+mean_partition_data);
//		Global.LOGGER.info("Standard deviation of Partition's data count: "+sd_partition_data);
//		Global.LOGGER.info("_____________________________________________________________________________");
//		Global.LOGGER.info("Average Server's data inflow: "+mean_server_inflow);
//		Global.LOGGER.info("Average Server's data outflow: "+mean_server_outflow);
//		Global.LOGGER.info("Average Server's data count: "+mean_server_data);
//		Global.LOGGER.info("Standard deviation of Server's data count: "+sd_server_data);
//		Global.LOGGER.info("_____________________________________________________________________________");
//		Global.LOGGER.info("Intra-server data movements: "+intra_server_dmv);
//		Global.LOGGER.info("Inter-server data movements: "+inter_server_dmv);
//		Global.LOGGER.info("Total data count: "+total_data);
		Global.LOGGER.info("*****************************************************************************");
		
	}
	
	public static PrintWriter getWriter() {		
		PrintWriter prWriter = null;
		
		try {
			prWriter = new PrintWriter(new BufferedWriter(new FileWriter(file, true)));			
		} catch(IOException e) {
			Global.LOGGER.error("Failed in reopening the metric file !!", e);
		}
		
		return prWriter;
	}
	
	public static int metricCollectionCycle = 0;
	
	public static void write() {
		int index = Metric.metricCollectionCycle;
		PrintWriter prWriter = Utility.getPrintWriter(Global.metric_dir, file);
		
		try {
			prWriter.print(index+" ");
			prWriter.print(mean_throughput.get(index)+" ");
			prWriter.print(mean_response_time.get(index)+" ");
			prWriter.print(percentage_dt.get(index)+" ");
			prWriter.print(mean_dti.get(index)+" ");
			prWriter.print(mean_partition_inflow.get(index)+" ");
			prWriter.print(mean_partition_outflow.get(index)+" ");
			prWriter.print(mean_partition_data.get(index)+" ");
			prWriter.print(sd_partition_data.get(index)+" ");
			prWriter.print(mean_server_inflow.get(index)+" ");
			prWriter.print(mean_server_outflow.get(index)+" ");
			prWriter.print(mean_server_data.get(index)+" ");
			prWriter.print(sd_server_data.get(index)+" ");
			prWriter.print(intra_server_dmv.get(index)+" ");
			prWriter.print(inter_server_dmv.get(index)+" ");			
			prWriter.print(total_data.get(index)+" ");
			prWriter.println();
			
		} finally {
			prWriter.close();
		}
		
		++Metric.metricCollectionCycle;
	}
}