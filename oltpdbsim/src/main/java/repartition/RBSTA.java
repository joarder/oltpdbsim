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
		pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MAX_IDT_REDUCTION());
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
			t.populateMovementList(cluster);
		else {						 // Movable non-DTs
			t.min_dmv = 0;
			t.max_idt_reduction = 0.0;
			t.processed = true;
		}
		
		return t;
	}
	
	// Checks whether processing current transaction affect any other transaction adversely
	public static boolean isAffected(WorkloadBatch wb, Tr t, MigrationPlan m) {			
		// Search the incident transactions for the targeted data rows to be moved
		for(int d : m.dataSet) {
			SimpleVertex v = wb.hgr.getVertex(d);
			
			for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
				Tr incidentT = tMap.get(h.getId());				
				
				if(!incidentT.equals(t) && incidentT.processed) {									
					int dst_server_id = m.to;
					
					if(incidentT.serverMap.containsKey(dst_server_id)) { // Either no change or potential reduction in the impact  
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
		
		return false;		
	}
	
	private static boolean isContainsAll(Tr incidentT, MigrationPlan m) {
		
		boolean contains = false;
		
		for(int src_server_id : m.fromSet) {
			if(incidentT.serverMap.containsKey(src_server_id))
				if(incidentT.serverMap.get(src_server_id).containsAll(m.dataSet))
					contains = true;
		}
		
		if(contains)
			return false;
		else
			return true;
	}
	
	public static void processTransaction(Cluster cluster, WorkloadBatch wb, Tr t, MigrationPlan m) {
				
		int dst_server_id = m.to;
		HashSet<Integer> dataSet = new HashSet<Integer>(m.dataSet);
		
		// Data migrations
		dataMigration(cluster, dst_server_id, dataSet);
		
		// Adjust transaction's serverSet				
		for(int src_server_id : m.fromSet) {
			for(int d : t.serverMap.get(src_server_id))
				t.serverMap.get(dst_server_id).add(d);			
			
			t.serverMap.remove(src_server_id);
		}
		
		// Update incident transactions
		for(int d : m.dataSet) {
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
		
		t.processed = true;
	} 
	
	// Perform data migrations
	private static void dataMigration(Cluster cluster, int dst_server_id, HashSet<Integer> dataSet) {
		// Chose the destination partition ids
		Server dst_server = cluster.getServer(dst_server_id);
		ArrayList<Partition> dst_partitionList = new ArrayList<Partition>();		
		
		for(int p : dst_server.getServer_partitions())
			dst_partitionList.add(cluster.getPartition(p));	

		Collections.sort(dst_partitionList, Partition.BY_DATA_SIZE());
		
		int dst_partition_id = 0;
		int r = 0;
		
		for(int d : dataSet) {
			Data data = cluster.getData(d);
	
			if(dataSet.size() > 1) {				
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

class Tr implements Comparable<Tr> {
	int id;		
	int min_dmv;
	double period;
	double max_idt_reduction;
	double max_lb_value;
	HashMap<Integer, HashSet<Integer>> serverMap;
	List<MigrationPlan> migrationPlanList;	
	boolean processed;
	
	Tr(int id, double period) {
		this.id = id;
		this.min_dmv = Integer.MAX_VALUE;
		this.period = period;
		this.max_idt_reduction = 0;
		this.max_lb_value = 0;
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
	
	void populateMovementList(Cluster cluster) {
		// Based on: https://code.google.com/p/combinatoricslib/
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(this.serverMap.keySet());

		// Create a simple permutation generator to generate N-permutations of the initial vector
		Generator<Integer> permutationGen = Factory.createPermutationWithRepetitionGenerator(initialVector, this.serverMap.size());		
		HashMap<HashSet<Integer>, Integer> uniqueFromSet = new HashMap<HashSet<Integer>, Integer>();
		
		// Get all possible N-permutations		
		HashSet<Integer> dataSet = new HashSet<Integer>();		
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
						dataSet.addAll(this.serverMap.get(from));
					}
					
					double idt_reduction_per_dmv = getIdtReductionImprovement(this, fromSet, req_dmv);
					double lb_improvement_per_dmv = getLbImprovement(cluster, this, fromSet, to, req_dmv);
					
					if(idt_reduction_per_dmv > this.max_idt_reduction)
						this.max_idt_reduction = idt_reduction_per_dmv;
					
					if(lb_improvement_per_dmv > this.max_lb_value)
						this.max_lb_value = lb_improvement_per_dmv;
					
					if(req_dmv < this.min_dmv)
						this.min_dmv = req_dmv;
					
					this.migrationPlanList.add(new MigrationPlan(fromSet, to, idt_reduction_per_dmv, lb_improvement_per_dmv, dataSet)); // From Source Server					
					
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
	}
	
	// Returns the idt reduction improvement
	static double getIdtReductionImprovement(Tr t, HashSet<Integer> fromSet, int req_dmv) {
		
		double old_idt = t.serverMap.size()*(1/t.period);
		double new_idt = (t.serverMap.size() - fromSet.size())*(1/t.period);
		double idt_improvement = old_idt - new_idt;
		
		return (idt_improvement/req_dmv);
	}

	// Returns the idt reduction improvement -- improved methodology 
	static double getIdtReductionImprovement(WorkloadBatch wb, Tr t, HashSet<Integer> fromSet, int req_dmv) {
		
		double new_idt = (t.serverMap.size() - fromSet.size())*(1/t.period);
		double incident_idt = 0.0;
		
		for(Entry<Integer, HashSet<Integer>> entry : t.serverMap.entrySet()) {
			
			HashSet<Integer> unique_trSet = new HashSet<Integer>();
			for(Integer d_id : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d_id);
				
				for(SimpleHEdge  h : wb.hgr.getIncidentEdges(v)) {
					Transaction incident_tr = wb.getTransaction(h.getId());
					
					if(!unique_trSet.contains(incident_tr.getTr_id())) {						
						incident_idt += incident_tr.getTr_serverSet().size()*(1/incident_tr.getTr_period());
					}
					
					unique_trSet.add(incident_tr.getTr_id());
				}			
			}
		}		
				
		double idt_improvement = new_idt + incident_idt;
		
		return (idt_improvement/req_dmv);
	}
	
	// Returns the lb improvement
	static double getLbImprovement(Cluster cluster, Tr t, HashSet<Integer> fromSet, int to, int req_dmv) {
		
		double old_lb = Metric.getServerLoadBalance(cluster);
		
		// Calculating new load-balance metric i.e. coefficient of variance
		DescriptiveStatistics server_data = new DescriptiveStatistics();
		
		for(Server s : cluster.getServers()) {
			if(t.serverMap.containsKey(s.getServer_id()) && fromSet.contains(s.getServer_id())) {
				int data_count = s.getServer_total_data() - t.serverMap.get(s.getServer_id()).size();
				server_data.addValue(data_count);
			} else if(t.serverMap.containsKey(to)) {
				int data_count = s.getServer_total_data() + t.serverMap.get(s.getServer_id()).size();
				server_data.addValue(data_count);
			}
		}
		
		double new_lb = server_data.getStandardDeviation()/server_data.getMean();
		double lb_improvement = old_lb - new_lb;
		
		return (lb_improvement/req_dmv);
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
	static Comparator<Tr> by_MAX_IDT_REDUCTION() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t1.max_idt_reduction > (int)t2.max_idt_reduction) ? -1 : 
					((int)t1.max_idt_reduction < (int)t2.max_idt_reduction) ? 1 : 0);
				//return Double.compare(t1.max_idt_reduction, t2.max_idt_reduction);
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
		return (">> T("+this.id+") | Minimum required # of data migrations ("+this.min_dmv+") | Min nomalized value ("+this.max_idt_reduction+") | "+this.serverMap);
	}
}

class MigrationPlan {
	HashSet<Integer> fromSet;	// From Server's Ids (for 1/2/3/...N span reductions)
	int to;		// To Server Id
	double idt_reduction_per_dmv;
	double lb_improvement_per_dmv;
	HashSet<Integer> dataSet;
	
	MigrationPlan(HashSet<Integer> fromSet, int to, double idt, double lb, HashSet<Integer> dataSet) {
		this.fromSet = new HashSet<Integer>(fromSet);
		this.to = to;
		this.idt_reduction_per_dmv = idt;
		this.lb_improvement_per_dmv = lb;
		this.dataSet = new HashSet<Integer>(dataSet);		
	}	
	
	@Override
	public String toString() {
		return (">> From("+this.fromSet+") | To("+this.to+") | Required # of data migrations("+this.dataSet.size()+") | Idt improvement("+this.idt_reduction_per_dmv+")");
	}
}