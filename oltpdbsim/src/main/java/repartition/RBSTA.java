package main.java.repartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.entry.Global;
import main.java.metric.Metric;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

/*
 * Repartition Based on Server-level Transactional Association (RBSTA)
 */

public class RBSTA {
	
	public static PriorityQueue<Tr> pq;
	public static HashMap<Integer, Tr> tMap;
	
	// Populates a priority queue to keep the potential transactions 
	public static void populatePQ(Cluster cluster, WorkloadBatch wb) {		
		pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MAX_IDT_REDUCTION_IMPROVEMENT());
		tMap = new HashMap<Integer, Tr>();
				
		for(SimpleHEdge h : wb.hgr.getEdges()) {			
			Tr t = prepare(cluster, wb, h);
			tMap.put(t.id, t);
			
			if(!t.processed)
				pq.add(tMap.get(t.id));
		}		
	}
	
	// Prepares current transaction for processing
	private static Tr prepare(Cluster cluster, WorkloadBatch wb, SimpleHEdge h) {
		Transaction tr = wb.getTransaction(h.getId());			
		Tr t = new Tr(tr.getTr_id(), tr.getTr_period());

		t.populateServerSet(cluster, tr);
		
		if(t.serverMap.size() > 1)	 // DTs
			t.populateMovementList(cluster, wb);
		else {						 // Movable non-DTs
			t.min_dmv = 0;
			t.max_idt_reduction_improvement = 0.0;
			t.processed = true;
		}
		
		return t;
	}
	
	// Checks whether processing current transaction affect any other transaction adversely
	public static boolean isAffected(WorkloadBatch wb, Tr t, MigrationPlan m) {			
		// Search the incident transactions for the targeted data rows to be moved
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d);
				
				for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
					Tr incidentT = tMap.get(h.getId());				
					
					if(!incidentT.equals(t) && incidentT.processed) {					
						if(incidentT.serverMap.containsKey(m.to)) { // Either no change or potential reduction in the impact  
							return false;
						} else { // Destination server is not covered by the incident transaction						
							if(!isContainsAll(incidentT, m)) // Either no change or potential reduction in the impact
								return false;
							else // Not all of the data ids from the source servers are included in the migration plan
								return true;
						}
					}
				}
			}
		}
		
		return false;		
	}
	
	private static boolean isContainsAll(Tr incidentT, MigrationPlan m) {
		
		boolean contains = false;
		
		for(int s_id : m.fromSet) {
			if(incidentT.serverMap.containsKey(s_id))
				if(incidentT.serverMap.get(s_id).containsAll(m.dataMap.get(s_id)))
					contains = true;
		}
		
		if(contains)
			return false;
		else
			return true;
	}
	
	public static void processTransaction(Cluster cluster, WorkloadBatch wb, Tr t, MigrationPlan m) {
						
		// Data migrations
		HashMap<Integer, HashSet<Integer>> dataMap = new HashMap<Integer, HashSet<Integer>>(m.dataMap);	
		dataMigration(cluster, m.to, dataMap);
		
		// Adjust transaction's serverSet				
		for(int s_id : m.fromSet) {
			for(int d : t.serverMap.get(s_id))
				t.serverMap.get(m.to).add(d);			
			
			t.serverMap.remove(s_id);
		}
		
		// Update incident transactions
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d);
	
				for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
					Tr incidentT = tMap.get(h.getId());
					
					if(!incidentT.equals(t) && !incidentT.processed) {				
						pq.remove(incidentT);
						tMap.remove(h.getId());
						
						Tr new_incidentT = prepare(cluster, wb, h);
						tMap.put(new_incidentT.id, new_incidentT);
						
						 // Only DTs will be added back after recalculations
						if(new_incidentT.serverMap.size() > 1)						
							pq.add(tMap.get(new_incidentT.id));					
					}
				}
			}
		}
		
		t.processed = true;
	} 
	
	// Perform data migrations
	private static void dataMigration(Cluster cluster, int dst_server_id, HashMap<Integer, HashSet<Integer>> dataMap) {
		// Chose the destination partition ids
		Server dst_server = cluster.getServer(dst_server_id);
		ArrayList<Partition> dst_partitionList = new ArrayList<Partition>();		
		
		for(int p : dst_server.getServer_partitions())
			dst_partitionList.add(cluster.getPartition(p));	

		Collections.sort(dst_partitionList, Partition.BY_DATA_SIZE());
		
		int dst_partition_id = 0;
		int r = 0;
		
		for(Entry<Integer, HashSet<Integer>> entry : dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				Data data = cluster.getData(d);
		
				if(dataMap.size() > 1) {				
					if(r == dst_partitionList.size())
						r = 0;
					
					dst_partition_id = dst_partitionList.get(r).getPartition_id();				
					++r;
					
				} else {
					dst_partition_id = dst_partitionList.get(0).getPartition_id();
				}
				
				DataMovement.migration(cluster, dst_server_id, dst_partition_id, data);		
		}
		}
	}
}

class Tr implements Comparable<Tr> {
	int id;		
	int min_dmv;
	double period;
	double max_idt_reduction_improvement;
	double max_lb_improvement;
	HashMap<Integer, HashSet<Integer>> serverMap;
	List<MigrationPlan> migrationPlanList;	
	boolean processed;
	
	Tr(int id, double period) {
		this.id = id;
		this.min_dmv = Integer.MAX_VALUE;
		this.period = period;
		this.max_idt_reduction_improvement = 0;
		this.max_lb_improvement = 0;
		this.serverMap = new HashMap<Integer, HashSet<Integer>>();
		this.migrationPlanList = new ArrayList<MigrationPlan>();
		this.processed = false;
	}
	
	void populateServerSet(Cluster cluster, Transaction tr) {
		for(int d_id : tr.getTr_dataSet()) {
			Data d = cluster.getData(d_id);
			
			int s_id = d.getData_server_id();
			
			if(this.serverMap.containsKey(s_id)) {
				this.serverMap.get(s_id).add(d.getData_id());
			} else {
				HashSet<Integer> dataSet = new HashSet<Integer>();
				dataSet.add(d.getData_id());
				this.serverMap.put(s_id, dataSet);
			}
		}
	}
	
	void populateMovementList(Cluster cluster, WorkloadBatch wb) {
		// Based on: https://code.google.com/p/combinatoricslib/
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(this.serverMap.keySet());

		// Create a simple permutation generator to generate N-permutations of the initial vector
		Generator<Integer> permutationGen = Factory.createPermutationWithRepetitionGenerator(initialVector, this.serverMap.size());		
		HashMap<HashSet<Integer>, Integer> uniqueFromSet = new HashMap<HashSet<Integer>, Integer>();
		
		// Get all possible N-permutations		
		HashMap<Integer, HashSet<Integer>> dataMap = new HashMap<Integer, HashSet<Integer>>();		
		int to = 0, req_dmv = 0;
		
		for (ICombinatoricsVector<Integer> permutations : permutationGen) {
			HashSet<Integer> fromSet = new HashSet<Integer>();		
			
			for(int i = 0; i < permutations.getSize()-1; i++)
				fromSet.add(permutations.getValue(i));			
						
			to = permutations.getValue(permutations.getSize() - 1);
			
			if(!fromSet.contains(to)) {
				if(!uniqueFromSet.containsKey(fromSet)
						|| (uniqueFromSet.containsKey(fromSet) && !uniqueFromSet.get(fromSet).equals(to))) {					

					//System.out.println("\t@ from("+fromSet+") --> to("+to+")");
					
					for(int from : fromSet) {						
						req_dmv += this.serverMap.get(from).size();
						dataMap.put(from, this.serverMap.get(from));
					}
					
					MigrationPlan m = new MigrationPlan(fromSet, to, dataMap, req_dmv);
					this.migrationPlanList.add(m); // From Source Server
					
					//double idt_reduction_per_dmv = getIdtReductionImprovement(this, fromSet, req_dmv);
					double idt_reduction_per_dmv = getIdtReductionImprovement(wb, this, m);					
					double lb_improvement_per_dmv = getLbImprovement(cluster, this, m);
					
					if(idt_reduction_per_dmv > this.max_idt_reduction_improvement)
						this.max_idt_reduction_improvement = idt_reduction_per_dmv;
					
					if(lb_improvement_per_dmv > this.max_lb_improvement)
						this.max_lb_improvement = lb_improvement_per_dmv;
					
					if(req_dmv < this.min_dmv)
						this.min_dmv = req_dmv;										
					
					m.idt_reduction_per_dmv = idt_reduction_per_dmv;
					m.lb_improvement_per_dmv = lb_improvement_per_dmv;					
					
					if(fromSet.size() > 1)
						uniqueFromSet.put(fromSet, to);
				} //end-if()
			} //end-if()	
		} // end-for(

		// Sorting in descending order by the maximum normalised value 
		Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
			@Override
			public int compare(MigrationPlan m1, MigrationPlan m2) {				
				return (((int)m1.idt_reduction_per_dmv > (int)m2.idt_reduction_per_dmv) ? -1 : 
					((int)m1.idt_reduction_per_dmv < (int)m2.idt_reduction_per_dmv) ? 1 : 0);				
			}
		});
				
		// Testing
		// After sorting
		/*System.out.println(">> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println(m.toString());
		}*/
		
		// Rank based on IDt priority (descending order)
		int idt_rank = this.migrationPlanList.size();
		for(int i = 0; i < this.migrationPlanList.size(); i++) {
			if(i != 0)
				if(this.migrationPlanList.get(i).idt_reduction_per_dmv != this.migrationPlanList.get(i-1).idt_reduction_per_dmv)
					--idt_rank;
			
			this.migrationPlanList.get(i).idt_rank = idt_rank * Global.rbsta_idt_priority; // Prioritise IDt
		}

		// Testing
		// After sorting
		/*System.out.println(">> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println(m.toString());
		}*/
		
		Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
			@Override
			public int compare(MigrationPlan m1, MigrationPlan m2) {				
				return (((int)m1.lb_improvement_per_dmv > (int)m2.lb_improvement_per_dmv) ? -1 : 
					((int)m1.lb_improvement_per_dmv < (int)m2.lb_improvement_per_dmv) ? 1 : 0);				
			}
		});
		
		// Rank based on Lb priority (descending order)
		int lb_rank = this.migrationPlanList.size();
		for(int i = 0; i < this.migrationPlanList.size(); i++) {
			if(i != 0)
				if(this.migrationPlanList.get(i).lb_improvement_per_dmv != this.migrationPlanList.get(i-1).lb_improvement_per_dmv)
					--lb_rank;
			
			this.migrationPlanList.get(i).lb_rank = lb_rank * Global.rbsta_lb_priority; // Prioritise Lb
		}

		// Assign combined rank		
		for(MigrationPlan m : this.migrationPlanList)
			m.combined_rank = (m.idt_rank + m.lb_rank);
		
		// Sort the array list by the value of combined rank (descending order)
		
		
		// Select the element with maximum combined rank value
		if(this.migrationPlanList.size() != 0) {			
			
			Global.LOGGER.info("----------------------------------------------");
			Global.LOGGER.info("Sorted list of potential migration plans:");
			
			for(MigrationPlan m : this.migrationPlanList)
				Global.LOGGER.info("--"+m.toString());
						
			MigrationPlan selected = this.migrationPlanList.get(0);
			
			Global.LOGGER.info("----------------------------------------------");
			Global.LOGGER.info("Selected migration plan: "+selected.toString());
		}
	}
	
	// Returns the idt reduction improvement
	static double getIdtReductionImprovement(Tr t, MigrationPlan m) {
		// Frequency over the past observation period: (Global.observationWindow/t.period)
		double old_idt = t.serverMap.size()*(1/t.period);
		double new_idt = (t.serverMap.size() - m.fromSet.size())*(1/t.period);
		double idt_improvement = old_idt - new_idt;
		
		return (idt_improvement/m.req_dmv);
	}

	// Returns the idt reduction improvement -- improved methodology 
	static double getIdtReductionImprovement(WorkloadBatch wb, Tr t, MigrationPlan m) {
		// Frequency over the past observation period: (Global.observationWindow/t.period)		
		double new_idt = (t.serverMap.size() - m.fromSet.size())*(1/t.period);
		double incident_idt = 0.0;
			
		for(Entry<Integer, HashSet<Integer>> entry : t.serverMap.entrySet()) {
			
			HashSet<Integer> unique_trSet = new HashSet<Integer>();
			
			for(Integer d_id : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d_id);
				
				for(SimpleHEdge  h : wb.hgr.getIncidentEdges(v)) {
					Transaction incident_tr = wb.getTransaction(h.getId());
					
					if(t.id != incident_tr.getTr_id()) {
						if(!unique_trSet.contains(incident_tr.getTr_id())) {
							
							//incident_idt += incident_tr.getTr_serverSet().size()*(1/incident_tr.getTr_period());
							
							// new code here
							incident_idt += getIdtReduction(incident_tr, m);
						}
											
						unique_trSet.add(incident_tr.getTr_id());
					}
				}			
			}
		}		
				
		double idt_improvement = new_idt + incident_idt;
		
		return (idt_improvement/m.req_dmv);
	}
	
	static double getIdtReduction(Transaction tr, MigrationPlan m) {
		
		double current_idt = (tr.getTr_serverSet().size())*(1/tr.getTr_period());
		
		// Perform a dummy movement to determine the new span
		Tr t = new Tr(tr.getTr_id(), tr.getTr_period());
		for(Entry<Integer, HashSet<Integer>> entry : tr.getTr_serverSet().entrySet()) {
			t.serverMap.put(entry.getKey(), entry.getValue());
		}
		
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			if(t.serverMap.containsKey(entry.getKey())) {
				for(Integer d_id : entry.getValue()) {
					if(t.serverMap.containsValue(d_id)) {
						t.serverMap.get(entry.getKey()).remove(d_id);
						
						if(t.serverMap.containsKey(m.to))
							t.serverMap.get(m.to).add(d_id);
						else {
							HashSet<Integer> dataSet = new HashSet<Integer>();
							dataSet.add(d_id);
							t.serverMap.put(m.to, dataSet);
						}
					}
				}
			}
		}
				
		double new_idt = (t.serverMap.size())*(1/t.period);
		
		if(new_idt <= current_idt)
			new_idt *= 1;
		else 
			new_idt *= -1;		
		
		return new_idt;
	}
	
	static boolean isContainsAllKeys(Transaction tr, HashSet<Integer> keys) {
		
		boolean contains = false;
		
		for(int key : keys) {
			if(tr.getTr_serverSet().containsKey(key))
					contains = true;
		}
		
		if(contains)
			return false;
		else
			return true;				
	}
	
	// Returns the lb improvement
	static double getLbImprovement(Cluster cluster, Tr t, MigrationPlan m) {
		
		double old_lb = Metric.getServerLoadBalance(cluster);
		
		// Calculating new load-balance metric i.e. coefficient of variance
		// The higher the CV, the greater the dispersion in the variable. 
		DescriptiveStatistics server_data = new DescriptiveStatistics();
		
		for(Server s : cluster.getServers()) {
			if(t.serverMap.containsKey(s.getServer_id()) && m.fromSet.contains(s.getServer_id())) {
				int data_count = s.getServer_total_data() - t.serverMap.get(s.getServer_id()).size();
				server_data.addValue(data_count);
			} else if(t.serverMap.containsKey(m.to)) {
				int data_count = s.getServer_total_data() + t.serverMap.get(m.to).size();
				server_data.addValue(data_count);
			}
		}
		
		double new_lb = server_data.getStandardDeviation()/server_data.getMean();
		double lb_improvement = old_lb - new_lb;
		
		return (lb_improvement/m.req_dmv);
	}
	
	// Ascending order
	static Comparator<Tr> by_MIN_DATA_MIGRATIONS() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t1.min_dmv < (int)t2.min_dmv) ? -1 : 
					((int)t1.min_dmv > (int)t2.min_dmv) ? 1 : 0);
			}			
		};
	}
	
	// Descending order
	static Comparator<Tr> by_MAX_IDT_REDUCTION_IMPROVEMENT() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t1.max_idt_reduction_improvement > (int)t2.max_idt_reduction_improvement) ? -1 : 
					((int)t1.max_idt_reduction_improvement < (int)t2.max_idt_reduction_improvement) ? 1 : 0);
			}			
		};
	}
	
	// Descending order
	static Comparator<Tr> by_MAX_LB_IMPROVEMENT() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t1.max_lb_improvement > (int)t2.max_lb_improvement) ? -1 : 
					((int)t1.max_lb_improvement < (int)t2.max_lb_improvement) ? 1 : 0);
			}			
		};
	}
	
	@Override
	public int compareTo(Tr t) {					
		return (((int)this.id > (int)t.id) ? -1 : 
			((int)this.id < (int)t.id) ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return (">> T"+this.id+": Minimum required # of data migrations ("+this.min_dmv+") "
				+ "| Max Idt reduction improvement ("+this.max_idt_reduction_improvement+") "
				+ "| Max Lb improvement ("+this.max_lb_improvement+") "
						+ "| "+this.serverMap);
	}
}

class MigrationPlan {
	HashSet<Integer> fromSet;	// From Server's Ids (for 1/2/3/...N span reductions)
	int to;		// To Server Id
	int req_dmv;
	double idt_reduction_per_dmv;
	double lb_improvement_per_dmv;
	HashMap<Integer, HashSet<Integer>> dataMap;
	double idt_rank;
	double lb_rank;
	double combined_rank;
	
	MigrationPlan(HashSet<Integer> fromSet, int to, HashMap<Integer, HashSet<Integer>> dataMap, int req_dmv) {
		this.fromSet = new HashSet<Integer>(fromSet);
		this.to = to;
		this.req_dmv = req_dmv;
		this.idt_reduction_per_dmv = 0.0;
		this.lb_improvement_per_dmv = 0.0;
		this.dataMap = new HashMap<Integer, HashSet<Integer>>(dataMap);
		this.idt_rank = 0.0;
		this.lb_rank = 0.0;
		this.combined_rank = 0.0;
	}	
	
	@Override
	public String toString() {
		return (">> From("+this.fromSet+") | To("+this.to+") | Required # of data migrations("+this.dataMap.size()+") | Idt improvement("+this.idt_reduction_per_dmv+") | Lb improvement ("+this.lb_improvement_per_dmv+")");
	}
}