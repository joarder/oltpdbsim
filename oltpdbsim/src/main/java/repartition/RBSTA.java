package main.java.repartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.entry.Global;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

/*
 * Repartition Based on Server-level Transactional Association (RBSTA)
 */

public class RBSTA {
	
	public static PriorityQueue<Tr> pq;
	public static HashMap<Integer, Tr> tMap;
	
	// Populates a priority queue to keep the potential transactions 
	public static void populatePQ(Cluster cluster, WorkloadBatch wb) {		
		
		if(Global.idt_priority == 1.0)		
			pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MAX_IDT_REDUCTION_IMPROVEMENT());
		else if((1 - Global.idt_priority) == 1.0)
			pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MAX_LB_IMPROVEMENT());
		else {
			if(Global.idt_priority > Global.lb_priority)
				pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MAX_IDT_REDUCTION_IMPROVEMENT());
			else
				pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MAX_LB_IMPROVEMENT());
		}
		
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
				
		if(t.dataMap.size() > 1)	 // DTs
			t.populateMovementList(cluster, wb);			
		else {						 // Movable non-DTs
			t.min_data_mgr = 0;
			t.max_idt_gain = 0.0;
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
						if(incidentT.dataMap.containsKey(m.to)) { // Either no change or potential reduction in the impact  
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
			if(incidentT.dataMap.containsKey(s_id))
				if(incidentT.dataMap.get(s_id).containsAll(m.dataMap.get(s_id)))
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
			for(int d : t.dataMap.get(s_id))
				t.dataMap.get(m.to).add(d);			
			
			t.dataMap.remove(s_id);
		}
		
		// Update incident transactions
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d);
				
				for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
					Tr incidentT = tMap.get(h.getId());
					//System.out.println("\t\t--> "+incidentT.toString());
					
					if(!incidentT.equals(t) && !incidentT.processed) {				
						pq.remove(incidentT);
						tMap.remove(h.getId());
						
						Tr new_incidentT = prepare(cluster, wb, h);
						tMap.put(new_incidentT.id, new_incidentT);				
						
						 // Only DTs will be added back after recalculations
						if(new_incidentT.dataMap.size() > 1)				
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
				
				DataMigration.migration(cluster, dst_server_id, dst_partition_id, data);		
			}
		}
	}
}

class Tr implements Comparable<Tr> {
	int id;		
	int min_data_mgr;
	double period;	
	double max_idt_gain; // Higher is better
	double max_lb_gain; // Lower is better		
	HashMap<Integer, HashSet<Integer>> dataMap;
	List<MigrationPlan> migrationPlanList;	
	boolean processed;
	
	static double max_idt_reduction_improvement;
	static double max_lb_improvement;
	
	Tr(int id, double period) {
		this.id = id;
		this.min_data_mgr = Integer.MAX_VALUE;
		this.period = period;
		this.max_idt_gain = 0;
		this.max_lb_gain = Integer.MAX_VALUE;
		this.dataMap = new HashMap<Integer, HashSet<Integer>>();
		this.migrationPlanList = new ArrayList<MigrationPlan>();
		this.processed = false;
	}
	
	void populateServerSet(Cluster cluster, Transaction tr) {
		for(int d_id : tr.getTr_dataSet()) {
			Data d = cluster.getData(d_id);			
			int s_id = d.getData_server_id();
			
			if(this.dataMap.containsKey(s_id)) {
				this.dataMap.get(s_id).add(d.getData_id());
			} else {
				HashSet<Integer> dataSet = new HashSet<Integer>();
				dataSet.add(d.getData_id());
				this.dataMap.put(s_id, dataSet);
			}
		}
	}
	
	@SuppressWarnings("rawtypes")
	static TreeSet idtRank;
	@SuppressWarnings("rawtypes")
	static TreeSet lbRank;	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void populateMovementList(Cluster cluster, WorkloadBatch wb) {
		// Based on: https://code.google.com/p/combinatoricslib/
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(this.dataMap.keySet());

		// Create a simple permutation generator to generate N-permutations of the initial vector
		Generator<Integer> permutationGen = Factory.createPermutationWithRepetitionGenerator(initialVector, this.dataMap.size());		
		HashMap<HashSet<Integer>, Integer> uniqueFromSet = new HashMap<HashSet<Integer>, Integer>();
		
		// Get all possible N-permutations		
		HashMap<Integer, HashSet<Integer>> dataMap;
		
		// Sets for preserving the ranks
		idtRank = new TreeSet();
		lbRank = new TreeSet();
		
		for (ICombinatoricsVector<Integer> permutations : permutationGen) {
			HashSet<Integer> fromSet = new HashSet<Integer>();		
			
			for(int i = 0; i < permutations.getSize()-1; i++)
				fromSet.add(permutations.getValue(i));			
						
			int to = permutations.getValue(permutations.getSize() - 1);
			
			if(!fromSet.contains(to)) {
				if(!uniqueFromSet.containsKey(fromSet)
						|| (uniqueFromSet.containsKey(fromSet) && !uniqueFromSet.get(fromSet).equals(to))) {
					
					dataMap = new HashMap<Integer, HashSet<Integer>>();
					int req_data_mgr = 0;
					
					for(int from : fromSet) {						
						req_data_mgr += this.dataMap.get(from).size();
						dataMap.put(from, this.dataMap.get(from));
					}										
					
					MigrationPlan m = new MigrationPlan(fromSet, to, dataMap, req_data_mgr);
					this.migrationPlanList.add(m); // From Source Server
					
					m.idt_gain_per_data_mgr = getIdtGain(wb, this, m);						
					m.lb_gain_per_data_mgr = getLbGain(cluster, this, m);					
					
					idtRank.add(m.idt_gain_per_data_mgr);
					lbRank.add(m.lb_gain_per_data_mgr);	
					
					if(fromSet.size() > 1)
						uniqueFromSet.put(fromSet, to);
				} //end-if()
			} //end-if()	
		} // end-for(

		// Get the maximum Idt and Lb gains for this transaction for normalization purpose
		max_idt_reduction_improvement = (double) idtRank.last();
		max_lb_improvement = (double) lbRank.last();
		
		// new code
		if(Global.idt_priority >= Global.lb_priority) {
			// Always take the one with maximum difference
			// Sort in descending order
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){				
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {
					m1.combined_weight = Math.abs((m1.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority
							- (m1.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority);
					m2.combined_weight = Math.abs((m2.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority
							- (m2.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority);					
		            
					return ((m1.combined_weight > m2.combined_weight) ? -1 : 
						(m1.combined_weight < m2.combined_weight) ? 1 : 0);
				}
			});			
		} else {
			/*
			 * If all are negative then take the largest one (sort in descending order)
			 * If all are positive then take the smallest one (sort in ascending order)
			 * If there is a mix of positives and negatives then take the one closest to zero
			 */			
			// Sort in ascending order
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){				
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {
					m1.combined_weight = Math.abs((m1.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority
							- (m1.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority);
					m2.combined_weight = Math.abs((m2.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority
							- (m2.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority);
					
					return ((m2.combined_weight > m1.combined_weight) ? -1 : 
						(m2.combined_weight < m1.combined_weight) ? 1 : 0);
				}
			});
		}		
		
		this.max_idt_gain = this.migrationPlanList.get(0).idt_gain_per_data_mgr;
		this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_data_mgr;
		this.min_data_mgr = this.migrationPlanList.get(0).req_data_mgr;
		
		// Testing
		System.out.println("-------------------------------------------------------------------------");
		System.out.println("Sorting based on combined ranking ...");
		System.out.println("--> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println("\t"+m.toString());
		}
	}

	// Returns the Idt gain per data movement 
	static double getIdtGain(WorkloadBatch wb, Tr t, MigrationPlan m) {
		
		double new_idt = (t.dataMap.size() - m.fromSet.size())*(1/t.period);		
		double incident_idt = 0.0;			
		
		for(Entry<Integer, HashSet<Integer>> entry : t.dataMap.entrySet()) {
			
			HashSet<Integer> unique_trSet = new HashSet<Integer>();
			
			for(Integer d_id : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d_id);
				
				for(SimpleHEdge  h : wb.hgr.getIncidentEdges(v)) {
					Transaction incident_tr = wb.getTransaction(h.getId());
					
					if(incident_tr.getTr_id() != t.id) {
						if(!unique_trSet.contains(incident_tr.getTr_id())) 
							incident_idt += getIdt(incident_tr, m);						
											
						unique_trSet.add(incident_tr.getTr_id());
					}
				}			
			}
		}		
						
		return ((new_idt + incident_idt)/m.req_data_mgr);
	}
	
	// Returns the expected Idt for a Transaction if a data migration plan would have been executed
	static double getIdt(Transaction tr, MigrationPlan m) {
		
		double original_idt = tr.getTr_serverSet().size()*(1/tr.getTr_period());
		
		// Execute a dummy data migration plan to determine the expected server-span
		Tr t = new Tr(tr.getTr_id(), tr.getTr_period());
		for(Entry<Integer, HashSet<Integer>> entry : tr.getTr_serverSet().entrySet()) {
			t.dataMap.put(entry.getKey(), entry.getValue());
		}
		
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			if(t.dataMap.containsKey(entry.getKey())) {
				for(Integer d_id : entry.getValue()) {
					if(t.dataMap.containsValue(d_id)) {
						t.dataMap.get(entry.getKey()).remove(d_id);
						
						if(t.dataMap.containsKey(m.to))
							t.dataMap.get(m.to).add(d_id);
						else {
							HashSet<Integer> dataSet = new HashSet<Integer>();
							dataSet.add(d_id);
							t.dataMap.put(m.to, dataSet);
						}
					}
				}
			}
		}
			
		double new_idt = t.dataMap.size()*(1/t.period);
		
		if(new_idt > original_idt)
			new_idt *= -1;
		
		// Return the expected Idt gain
		return new_idt;
	}
	
	// Returns the lb gain
	static double getLbGain(Cluster cluster, Tr t, MigrationPlan m) {		
		DescriptiveStatistics new_server_data = new DescriptiveStatistics();
		
		for(Server s : cluster.getServers()) {
			if(t.dataMap.containsKey(s.getServer_id())) {
				
				if(m.fromSet.contains(s.getServer_id())) {
					int data_count = s.getServer_total_data() - t.dataMap.get(s.getServer_id()).size();
					new_server_data.addValue(data_count);
					
				} else if(m.to == s.getServer_id()) {
					int data_count = s.getServer_total_data() + t.dataMap.get(s.getServer_id()).size();
					new_server_data.addValue(data_count);
				}
			}
		}

		return (new_server_data.getVariance()/m.req_data_mgr);
	}
	
	// Descending order
	static Comparator<Tr> by_MAX_IDT_REDUCTION_IMPROVEMENT() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t1.max_idt_gain > (int)t2.max_idt_gain) ? -1 : 
					((int)t1.max_idt_gain < (int)t2.max_idt_gain) ? 1 : 0);
			}			
		};
	}
	
	// Ascending order
	static Comparator<Tr> by_MAX_LB_IMPROVEMENT() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t2.max_lb_gain > (int)t1.max_lb_gain) ? -1 : 
					((int)t2.max_lb_gain < (int)t1.max_lb_gain) ? 1 : 0);
			}			
		};
	}
		
	// Ascending order
	static Comparator<Tr> by_MIN_DATA_MIGRATIONS() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t2.min_data_mgr > (int)t1.min_data_mgr) ? -1 : 
					((int)t2.min_data_mgr < (int)t1.min_data_mgr) ? 1 : 0);
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
		return (">> T"+this.id+": Min required data migrations ("+this.min_data_mgr+") "
				+ "| Max Idt gain ("+this.max_idt_gain+") "
				+ "| Max Lb gain ("+this.max_lb_gain+") "
						+ "| "+this.dataMap);
	}
}