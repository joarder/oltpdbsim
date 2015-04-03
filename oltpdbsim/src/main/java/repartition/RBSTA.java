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
import main.java.entry.Global;
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
		
		pq = new PriorityQueue<Tr>(wb.hgr.getEdges().size(), Tr.by_MIN_DATA_MIGRATIONS());
		tMap = new HashMap<Integer, Tr>();
		
		for(SimpleHEdge h : wb.hgr.getEdges()) {			
			Tr t = prepare(cluster, wb, h);
			tMap.put(t.id, t);		
			pq.add(tMap.get(t.id));
		}
	}
	
	// Prepares current transaction for processing
	private static Tr prepare(Cluster cluster, WorkloadBatch wb, SimpleHEdge h) {
		Transaction tr = wb.getTransaction(h.getId());			
		Tr t = new Tr(tr.getTr_id(), tr.getTr_period(), tr.getTr_idt());

		t.populateServerSet(cluster, tr);
		
		if(t.serverMap.size() > 1)	 // DTs
			t.populateMovementList();
		else						 // Movable non-DTs
			t.min_data_migration = 0;
		
		return t;
	}
	
	// Checks whether processing current transaction affect any other transaction adversely
	public static boolean isAffected(WorkloadBatch wb, Tr t, MigrationPlan m) {			
		// Search the incident transactions for the targeted data rows to be moved
		for(int d : m.dataSet) {
			SimpleVertex v = wb.hgr.getVertex(d);
			
			for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
				Tr incidentT = tMap.get(h.getId());
				
				if(incidentT.equals(t)) {									
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
				
				if(!incidentT.equals(t)) {				
					pq.remove(incidentT);
					tMap.remove(h.getId());
					
					Tr new_incidentT = prepare(cluster, wb, h);				
					tMap.put(new_incidentT.id, new_incidentT);
					pq.add(tMap.get(new_incidentT.id));
				}
			}
		}
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
	HashMap<Integer, HashSet<Integer>> serverMap;
	List<MigrationPlan> migrationPlanList;	
	
	Tr(int id, double period, double old_impact) {
		this.id = id;
		this.min_data_migration = Integer.MAX_VALUE;
		this.serverMap = new HashMap<Integer, HashSet<Integer>>();
		this.migrationPlanList = new ArrayList<MigrationPlan>();
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
		int span_reduction_factor = Global.rbsta_span_reduction;
		Generator<Integer> gen;
		
		if(this.serverMap.size() < span_reduction_factor+1)
			gen = Factory.createPermutationWithRepetitionGenerator(initialVector, this.serverMap.size());
		else
			gen = Factory.createPermutationWithRepetitionGenerator(initialVector, span_reduction_factor+1);
		
		// Get all possible N-permutations
		HashSet<HashSet<Integer>> sameFromSet = new HashSet<HashSet<Integer>>();
		HashSet<Integer> dataSet = new HashSet<Integer>();
		int to = 0, req_dmv = 0;
		System.out.println("---------------------------------------------------------------");
		for (ICombinatoricsVector<Integer> valueSet : gen) {
			HashSet<Integer> fromSet = new HashSet<Integer>();
			
			for(int i = 0; i < valueSet.getSize()-1; i++)
				fromSet.add(valueSet.getValue(i));			
						
			to = valueSet.getValue(valueSet.getSize()-1);
			
			//System.out.println("@ fromSet="+fromSet+"|to="+to);
			
			if(!fromSet.contains(to)) {				
				if(!sameFromSet.contains(fromSet)) {
					
					if(this.serverMap.size() < span_reduction_factor+1) {
						
						if(fromSet.size() == this.serverMap.size()-1) {
						
							for(int from : fromSet) {				
								req_dmv += this.serverMap.get(from).size();
								dataSet.addAll(this.serverMap.get(from));
							}
							
							if(req_dmv < this.min_data_migration)
								this.min_data_migration = req_dmv;
											
							this.migrationPlanList.add(new MigrationPlan(fromSet, to, dataSet)); // From Source Server
							
							if(this.serverMap.keySet().size() > 2 && span_reduction_factor > 1)						
								sameFromSet.add(new HashSet<Integer>(fromSet));
						}
						
					} else {
						if(fromSet.size() == span_reduction_factor) {				
							//if(fromSet.size() >= this.serverMap.size()) { // Allowed when?
							
								for(int from : fromSet) {				
									req_dmv += this.serverMap.get(from).size();
									dataSet.addAll(this.serverMap.get(from));
								}
								
								if(req_dmv < this.min_data_migration)
									this.min_data_migration = req_dmv;
												
								this.migrationPlanList.add(new MigrationPlan(fromSet, to, dataSet)); // From Source Server
								
								if(this.serverMap.keySet().size() > 2 && span_reduction_factor > 1)						
									sameFromSet.add(new HashSet<Integer>(fromSet));
							//}
						}
					}
				}
			}				
		}
		
		// Testing
		System.out.println("-- T"+this.id+"|"+this.serverMap+"|"+this.min_data_migration);
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println(m.toString());
		}
		
		// Sort the list in ascending order of required numbers of data movement
		Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
			@Override
			public int compare(MigrationPlan m1, MigrationPlan m2) {					
				return (((int)m1.dataSet.size() < (int)m2.dataSet.size()) ? -1 : 
        			((int)m1.dataSet.size() > (double)m2.dataSet.size()) ? 1 : 0);
			}
		});
		
		// Testing
		// After sorting
		/*System.out.println(">> T"+this.id+"|"+this.min_dmv+"|"+this.impact);
		for(Move m : this.moveList) {
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
	
	@Override
	public int compareTo(Tr t) {					
		return (((int)this.id > (int)t.id) ? -1 : 
			((int)this.id < (int)t.id) ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return (">> T("+this.id+") | Minimum required # of data migrations ("+this.min_data_migration+") | "+this.serverMap);
	}
}

class MigrationPlan {
	HashSet<Integer> fromSet;	// From Server's Ids (for 1/2/3/...N span reductions)
	int to;		// To Server Id
	HashSet<Integer> dataSet;
	
	MigrationPlan(HashSet<Integer> fromSet, int to, HashSet<Integer> dataSet) {
		this.fromSet = new HashSet<Integer>(fromSet);
		this.to = to;
		this.dataSet = new HashSet<Integer>(dataSet);
	}	
	
	@Override
	public String toString() {
		return (">> From("+this.fromSet+") | To("+this.to+") | Required # of data migrations ("+this.dataSet.size()+")");
	}
}