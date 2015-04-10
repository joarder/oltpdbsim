package main.java.repartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.Server;
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
		pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MIN_NORM_VALUES());
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
			t.populateMovementList();
		else {						 // Movable non-DTs
			t.min_data_migration = 0;
			t.min_data_migr_value = 0.0;
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
	int min_data_migration;
	double period;
	double min_data_migr_value;
	HashMap<Integer, HashSet<Integer>> serverMap;
	List<MigrationPlan> migrationPlanList;	
	boolean processed;
	
	Tr(int id, double period) {
		this.id = id;
		this.min_data_migration = Integer.MAX_VALUE;
		this.period = period;
		this.min_data_migr_value = Double.MAX_VALUE;
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
	
	void populateMovementList() {
		// Based on: https://code.google.com/p/combinatoricslib/
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = 
				Factory.createVector(this.serverMap.keySet());

		// Create a simple permutation generator to generate N-permutations of the initial vector
		Generator<Integer> gen = Factory.createPermutationWithRepetitionGenerator(initialVector, this.serverMap.size());
		
		HashMap<HashSet<Integer>, Integer> uniqueFromSet = new HashMap<HashSet<Integer>, Integer>();
		
		// Get all possible N-permutations		
		HashSet<Integer> dataSet = new HashSet<Integer>();
		int to = 0, req_dmv = 0;
		double val = 0.0d, min_val = 0.0d;

		for (ICombinatoricsVector<Integer> valueSet : gen) {
			HashSet<Integer> fromSet = new HashSet<Integer>();			
			
			for(int i = 0; i < valueSet.getSize()-1; i++)
				fromSet.add(valueSet.getValue(i));			
						
			to = valueSet.getValue(valueSet.getSize() - 1);
			
			if(!fromSet.contains(to)) {
				if(!uniqueFromSet.containsKey(fromSet)
						|| (uniqueFromSet.containsKey(fromSet) && !uniqueFromSet.get(fromSet).equals(to))) {
					
					for(int from : fromSet) {				
						val = this.serverMap.get(from).size();
						val = val/fromSet.size();
						val = val/(1/this.period);
						min_val += val;
						
						req_dmv += this.serverMap.get(from).size();
						dataSet.addAll(this.serverMap.get(from));
					}
					
					if(min_val < this.min_data_migr_value)
						this.min_data_migr_value = min_val;
					
					if(req_dmv < this.min_data_migration)
						this.min_data_migration = req_dmv;
					
					this.migrationPlanList.add(new MigrationPlan(fromSet, to, dataSet, val)); // From Source Server					
					
					if(fromSet.size() > 1)
						uniqueFromSet.put(fromSet, to);	
				} //end-if()
			} //end-if()	
		} // end-for(

		// Sorting in ascending order by the minimum normalised value 
		Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
			@Override
			public int compare(MigrationPlan m1, MigrationPlan m2) {				
				return Double.compare(m1.norm_value, m2.norm_value);				
			}
		});
				
		// Testing
		// After sorting
		/*System.out.println(">> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println(m.toString());
		}*/
	}
	
	// Ascending order
	static Comparator<Tr> by_MIN_DATA_MIGRATIONS() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				return (((int)t1.min_data_migration < (int)t2.min_data_migration) ? -1 : 
					((int)t1.min_data_migration > (int)t2.min_data_migration) ? 1 : 0);
			}			
		};
	}
	
	// Ascending order
	static Comparator<Tr> by_MIN_NORM_VALUES() {
		return new Comparator<Tr>() {
			@Override
			public int compare(Tr t1, Tr t2) {
				/*return (((int)t1.min_data_migration < (int)t2.min_data_migration) ? -1 : 
					((int)t1.min_data_migration > (int)t2.min_data_migration) ? 1 : 0);*/
				return Double.compare(t1.min_data_migr_value, t2.min_data_migr_value);
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
		return (">> T("+this.id+") | Minimum required # of data migrations ("+this.min_data_migration+") | Min nomalized value ("+this.min_data_migr_value+") | "+this.serverMap);
	}
}

class MigrationPlan {
	HashSet<Integer> fromSet;	// From Server's Ids (for 1/2/3/...N span reductions)
	int to;		// To Server Id
	double norm_value;
	HashSet<Integer> dataSet;
	
	MigrationPlan(HashSet<Integer> fromSet, int to, HashSet<Integer> dataSet, double val) {
		this.fromSet = new HashSet<Integer>(fromSet);
		this.to = to;
		this.dataSet = new HashSet<Integer>(dataSet);
		this.norm_value = val;
	}	
	
	@Override
	public String toString() {
		return (">> From("+this.fromSet+") | To("+this.to+") | Required # of data migrations ("+this.dataSet.size()+") | Normalized Value ("+this.norm_value+")");
	}
}