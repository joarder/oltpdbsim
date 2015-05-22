package main.java.metric;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;

import main.java.cluster.Cluster;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.entry.Global;
import main.java.utils.Utility;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import umontreal.iro.lecuyer.simevents.Sim;

public class Metric implements java.io.Serializable {
	
	private static final long serialVersionUID = 2422722688394703455L;
	
	public static ArrayList<Double> time;
	
	public static ArrayList<Double> mean_throughput;
	
	public static ArrayList<Double> idt;
	public static ArrayList<Double> percentage_dt;
	public static ArrayList<Double> percentage_ndt;
	
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
	
	public static ArrayList<Integer> current_tr;
	public static ArrayList<Integer> current_dt;
	public static ArrayList<Integer> current_ndt;
	
	public static ArrayList<Integer> total_edges;
	public static ArrayList<Integer> total_vertices;
	public static ArrayList<Integer> edges_in_cut;
	
	private static File file;
	private static PrintWriter prWriter;
		
	public static int metricCollectionCycle = 0;
	
	public static void init() {
		time = new ArrayList<Double>();
		
		mean_throughput = new ArrayList<Double>();
		
		idt = new ArrayList<Double>();
		percentage_dt = new ArrayList<Double>();
		percentage_ndt = new ArrayList<Double>();
		
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
		
		current_tr = new ArrayList<Integer>();
		current_dt = new ArrayList<Integer>();
		current_ndt = new ArrayList<Integer>();
		
		total_edges = new ArrayList<Integer>();
		total_vertices = new ArrayList<Integer>();
		edges_in_cut = new ArrayList<Integer>();
		
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
				
		time.add(Sim.time()/(double)Global.observationWindow);
		
		mean_throughput.add(wb.get_throughput());

		idt.add(wb.getIdt());
		percentage_dt.add(wb.get_percentage_dt());
		percentage_ndt.add(wb.get_percentage_ndt());
		
		intra_server_dmv.add(wb.get_intra_dmv());
		inter_server_dmv.add(wb.get_inter_dmv());

		current_tr.add(wb.get_tr_nums());
		current_dt.add(wb.get_dt_nums());
		current_ndt.add(wb.get_ndt_nums());
		
		total_edges.add(wb.hgr.getEdgeCount());
		total_vertices.add(wb.hgr.getVertexCount());
		edges_in_cut.add(0);
		
		getServerStatistic(cluster);
		getPartitionStatistic(cluster);
	}
	
	// Returns the Coefficient of Variance on server data
	public static double getCVServerData(Cluster cluster) {		
		DescriptiveStatistics server_data = new DescriptiveStatistics();
		
		for(Server server : cluster.getServers())
			server_data.addValue(server.getServer_total_data());
		
		double c_v = server_data.getStandardDeviation()/server_data.getMean();
		return c_v; 
	}
	
	// Returns the CMean on server data
	public static double getCVServerData(Cluster cluster, Transaction tr) {		
		DescriptiveStatistics server_data = new DescriptiveStatistics();
		
		for(Entry<Integer, HashSet<Integer>> entry : tr.getTr_serverSet().entrySet()) {
			server_data.addValue(entry.getValue().size());
		}			
		
		double c_v = server_data.getStandardDeviation()/server_data.getMean();
		return c_v;		
	}
	
	// Returns the Mean on server data
	public static double getMeanServerData(Cluster cluster) {		
		DescriptiveStatistics server_data = new DescriptiveStatistics();
		
		for(Server server : cluster.getServers())
			server_data.addValue(server.getServer_total_data());
		
		return server_data.getMean();		
	}
	
	// Returns the Mean on server data
	public static double getMeanServerData(Cluster cluster, Transaction tr) {		
		DescriptiveStatistics server_data = new DescriptiveStatistics();
		
		for(Entry<Integer, HashSet<Integer>> entry : tr.getTr_serverSet().entrySet()) {
			server_data.addValue(entry.getValue().size());
		}			
		
		return server_data.getMean();		
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
		Global.LOGGER.info("Simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
		
		if(Global.incrementalRepartitioning)
			Global.LOGGER.info("Incremental repartitioning cycle: "+Global.repartitioningCycle);
		
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Workload variation: "+Global.percentageChangeInWorkload);
		Global.LOGGER.info("Variation adjustment: "+Global.adjustment);
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Total transactions processed: "+Global.total_transactions);
		Global.LOGGER.info("Total unique transactions processed: "+Global.global_trSeq);
		Global.LOGGER.info("Total unique transactions removed: "+Global.remove_count);	
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Total transactions in the current observation window: "+current_tr);
		Global.LOGGER.info("Total DT in the current observation window: "+current_dt);
		Global.LOGGER.info("Total non-DT in the current observation window: "+current_ndt);
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Hypergraph size: ");
		Global.LOGGER.info("Edges: "+total_edges);
		Global.LOGGER.info("Vertices: "+total_vertices);
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Average throughput: "+mean_throughput+" TPS");
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("IDt: "+idt);
		Global.LOGGER.info("Percentage of distributed transactions: "+percentage_dt+" %");
		Global.LOGGER.info("Percentage of non-distributed transactions: "+percentage_ndt+" %");
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Average Partition's data inflow: "+mean_partition_inflow);
		Global.LOGGER.info("Average Partition's data outflow: "+mean_partition_outflow);
		Global.LOGGER.info("Average Partition's data count: "+mean_partition_data);
		Global.LOGGER.info("Standard deviation of Partition's data count: "+sd_partition_data);
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Average Server's data inflow: "+mean_server_inflow);
		Global.LOGGER.info("Average Server's data outflow: "+mean_server_outflow);
		Global.LOGGER.info("Average Server's data count: "+mean_server_data);
		Global.LOGGER.info("Standard deviation of Server's data count: "+sd_server_data);
		Global.LOGGER.info("_____________________________________________________________________________");
		Global.LOGGER.info("Intra-server data movements: "+intra_server_dmv);
		Global.LOGGER.info("Inter-server data movements: "+inter_server_dmv);
		Global.LOGGER.info("Total data count: "+total_data);
		//Global.LOGGER.info("*****************************************************************************");
		
	}
	
	public static void write() {
		int index = Metric.metricCollectionCycle;		
		prWriter = Utility.getPrintWriter(Global.metric_dir, file);
		
		try {
			prWriter.append(index+" ");			
			prWriter.append(time.get(index)+" ");
			prWriter.append(idt.get(index)+" ");
			prWriter.append(mean_server_inflow.get(index)+" ");			
			prWriter.append(mean_server_data.get(index)+" ");
			prWriter.append(sd_server_data.get(index)+" ");			
			prWriter.append(inter_server_dmv.get(index)+" ");		
			prWriter.append(total_data.get(index)+"\n");
			
			/*prWriter.append(mean_throughput.get(index)+" ");
			prWriter.append(current_dt.get(index)+" ");
			prWriter.append(percentage_dt.get(index)+" ");
			prWriter.append(mean_server_outflow.get(index)+" ");
			prWriter.append(intra_server_dmv.get(index)+" ");
			prWriter.append(mean_partition_inflow.get(index)+" ");
			prWriter.append(mean_partition_outflow.get(index)+" ");
			prWriter.append(mean_partition_data.get(index)+" ");
			prWriter.append(sd_partition_data.get(index)+" ");*/			
			
		} finally {
			prWriter.close();
		}
		
		++Metric.metricCollectionCycle;
	}
}