/*******************************************************************************
 * Copyright [2014] [Joarder Kamal]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package main.java.metric;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;

import main.java.cluster.Cluster;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.entry.Global;
import main.java.utils.Utility;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import umontreal.iro.lecuyer.simevents.Sim;

public class Metric {	
	
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
	
	public static ArrayList<Integer> inter_server_data_mgr;
	public static ArrayList<Integer> intra_server_data_mgr;
		
	// New -- server-level pair-wise data migration counts
	public static HashMap<Pair<Integer, Integer>, Integer> mutually_exclusive_serverSets;
	public static ArrayList<Double> mean_pairwise_inter_server_data_mgr;
	public static ArrayList<Double> sd_pairwise_inter_server_data_mgr;
	public static ArrayList<Integer> estimated_data_mgr;
	
	// New -- computational time
	public static ArrayList<Long> repartitioning_time;
	
	public static ArrayList<Integer> current_tr;
	public static ArrayList<Integer> current_dt;
	public static ArrayList<Integer> current_ndt;
	
	public static ArrayList<Integer> total_edges;
	public static ArrayList<Integer> total_vertices;
	public static ArrayList<Integer> edges_in_cut;
	
	private static File file;
	private static PrintWriter prWriter;
		
	public static int metricCollectionCycle = 0;
	
	public static void init(Cluster cluster) {
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
		
		inter_server_data_mgr = new ArrayList<Integer>();
		intra_server_data_mgr = new ArrayList<Integer>();		
				
		// New -- mutually exclusive server sets
		mutually_exclusive_serverSets = new HashMap<Pair<Integer, Integer>, Integer>();
		mean_pairwise_inter_server_data_mgr = new ArrayList<Double>();
		sd_pairwise_inter_server_data_mgr = new ArrayList<Double>();
		estimated_data_mgr = new ArrayList<Integer>();
		
		// New -- computational time
		repartitioning_time = new ArrayList<Long>();
		
		current_tr = new ArrayList<Integer>();
		current_dt = new ArrayList<Integer>();
		current_ndt = new ArrayList<Integer>();
		
		total_edges = new ArrayList<Integer>();
		total_vertices = new ArrayList<Integer>();
		edges_in_cut = new ArrayList<Integer>();
		
		// Creating a metric file
		file = new File(Global.metric_dir+"run"+Global.repeatedRuns+"/"
					+Global.simulation+"-s"+Global.servers+"-p"+Global.partitions+".out");
		
		prWriter = Utility.getPrintWriter(Global.metric_dir, file);		
	}
	
	public static void initServerSet(Cluster cluster) {
		
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(cluster.getServerSet());		
		Generator<Integer> gen = Factory.createSimpleCombinationGenerator(initialVector, 2);
		
		for (ICombinatoricsVector<Integer> combination : gen) {			
		
			int s_a_id = combination.getValue(0);
			int s_b_id = combination.getValue(1);
			
			Pair<Integer, Integer> pair1 = new ImmutablePair<Integer, Integer>(s_a_id, s_b_id);
			Pair<Integer, Integer> pair2 = new ImmutablePair<Integer, Integer>(s_b_id, s_a_id);
			
			mutually_exclusive_serverSets.put(pair1, 0);			
			mutually_exclusive_serverSets.put(pair2, 0);
		}
		
		// Testing
		//System.out.println(mutually_exclusive_serverSets);
	}
	
	public static void reInitServerSet() {
		for(Entry<Pair<Integer, Integer>, Integer> entry : mutually_exclusive_serverSets.entrySet()) {
			mutually_exclusive_serverSets.put(entry.getKey(), 0);
		}
	}
	
	public static void collect(Cluster cluster, WorkloadBatch wb) {
				
		time.add(Sim.time()/(double)Global.observationWindow);
		
		mean_throughput.add(wb.get_throughput());

		idt.add(wb.getIdt());
		percentage_dt.add(wb.get_percentage_dt());
		percentage_ndt.add(wb.get_percentage_ndt());
		
		intra_server_data_mgr.add(wb.get_intra_dmv());
		inter_server_data_mgr.add(wb.get_inter_dmv());

		current_tr.add(wb.get_tr_nums());
		current_dt.add(wb.get_dt_nums());
		current_ndt.add(wb.get_ndt_nums());
		
		total_edges.add(wb.hgr.getEdgeCount());
		total_vertices.add(wb.hgr.getVertexCount());
		edges_in_cut.add(0);
		
		getServerStatistic(cluster);
		getPartitionStatistic(cluster);
		
		repartitioning_time.add(wb.get_repartitioning_time());
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
		DescriptiveStatistics _meDMV = new DescriptiveStatistics();
		
		for(Server server : cluster.getServers()) {			
			_data.addValue(server.getServer_total_data());
			_inflow.addValue(server.getServer_inflow());
			_outflow.addValue(server.getServer_outflow());				
		}

		// New - mutually exclusive server sets
		ArrayList<Integer> estimated_dmgr = new ArrayList<Integer>();
		for(Entry<Pair<Integer, Integer>, Integer> entry : mutually_exclusive_serverSets.entrySet()) {
			_meDMV.addValue(entry.getValue());
			estimated_dmgr.add(entry.getValue());
		}
		
		// Sort the list containing data migration counts within individual server pairs
		Collections.sort(estimated_dmgr);
		
		// Sum up every floor(S/2) index values to get the estimated data migration time
		int estimated_mgr = 0;
		int j = 0;
		for(int i = 0; i < estimated_dmgr.size(); ++i) {
			j = (int) (i+Math.floor(Global.servers/2));
			estimated_mgr += estimated_dmgr.get(i);
			i = j;
		}
						
		mean_server_inflow.add(_inflow.getMean());
		mean_server_outflow.add(_outflow.getMean());
		mean_server_data.add(_data.getMean());
		sd_server_data.add(_data.getStandardDeviation());
		total_data.add((long) Global.global_dataCount);
		
		mean_pairwise_inter_server_data_mgr.add(_meDMV.getMean());
		sd_pairwise_inter_server_data_mgr.add(_meDMV.getStandardDeviation());
		estimated_data_mgr.add(estimated_mgr);
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
		
		mean_partition_inflow.add(_inflow.getMean());
		mean_partition_outflow.add(_outflow.getMean());
		mean_partition_data.add(_data.getMean());
		sd_partition_data.add(_data.getStandardDeviation());		
	}
	
	public static void report() {
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");		
		Global.LOGGER.info("Reporting simulation statistic ...");
		Global.LOGGER.info("Simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
		
		if(Global.incrementalRepartitioning)
			Global.LOGGER.info("Incremental repartitioning cycle: "+Global.repartitioningCycle);

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
		Global.LOGGER.info("Intra-server data movements: "+intra_server_data_mgr);
		Global.LOGGER.info("Inter-server data movements: "+inter_server_data_mgr);
		Global.LOGGER.info("Total data count: "+total_data);			
		//Global.LOGGER.info("*****************************************************************************");		
	}

	/*public static int getMESValue(int left, int right) {
		for(Entry<Pair<Integer, Integer>, Integer> entry : mutually_exclusive_serverSets.entrySet()) {
			if(entry.getKey().getLeft() == left && entry.getKey().getRight() == right)
				return entry.getValue();
		}
		
		return 0;
	}*/
	
	public static void updateMESValue(int left, int right) {
		Pair<Integer, Integer> key = new ImmutablePair<Integer, Integer>(left, right);	
		/*System.out.println(mutually_exclusive_serverSets);
		System.out.println(mutually_exclusive_serverSets.get(key));*/
		mutually_exclusive_serverSets.put(key, mutually_exclusive_serverSets.get(key) + 1);
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
			prWriter.append(inter_server_data_mgr.get(index)+" ");		
			prWriter.append(total_data.get(index)+" ");
			prWriter.append(mean_pairwise_inter_server_data_mgr.get(index)+" ");
			prWriter.append(sd_pairwise_inter_server_data_mgr.get(index)+" ");
			prWriter.append(estimated_data_mgr.get(index)+" ");
			prWriter.append(repartitioning_time.get(index)+"\n");
			
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
		
	public static void close() {
		prWriter.close();
	}
}