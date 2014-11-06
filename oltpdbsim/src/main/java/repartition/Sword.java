package main.java.repartition;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.VirtualData;
import main.java.utils.graph.CompressedHEdge;
import main.java.utils.queue.PQ;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class Sword {	
	
	// <v_id, <s_id, sum of weights of transactions incident of v_id>> -- <v_id, <s_id, nh>>
	public SortedMap<Integer, SortedMap<Integer, Integer>> dt_cnSet; 	
	public SortedMap<Integer, SortedMap<Integer, Integer>> ndt_cnSet;
	
	public PQ<CompressedHEdge> pq; 	// Priority Queue of Hyperedges
	public Set<Integer> v_cut;
	
	public Sword() {
		dt_cnSet = new TreeMap<Integer, SortedMap<Integer, Integer>>();
		ndt_cnSet = new TreeMap<Integer, SortedMap<Integer, Integer>>();
		
		pq = new PQ<CompressedHEdge>(1000, Collections.reverseOrder());
		v_cut = new TreeSet<Integer>();
	}
	
	@SuppressWarnings("unused")
	public void init(Cluster cluster, WorkloadBatch wb) {
		
		double c_e = 0;
		int total_tr_contribution = 0;		
		int ndt_e = 0;		
		
		// Determine the total ndt
		for(Entry<Integer, Map<Integer, Transaction>> entry : wb.getTrMap().entrySet()) {
			for(Entry<Integer, Transaction> tr_entry : entry.getValue().entrySet()) {				
				Transaction tr = tr_entry.getValue();
				
				tr.calculateSpans(cluster);
				total_tr_contribution += tr.getTr_frequency();						
			}
		}
		
		for(Entry<Integer, Map<Integer, Transaction>> entry : wb.getTrMap().entrySet()) {
			for(Entry<Integer, Transaction> tr_entry : entry.getValue().entrySet()) {				
				Transaction tr = tr_entry.getValue();
				
				ndt_e = tr.getTr_frequency();
				
				if(tr.isDt()) { // DT -- first set of CN
					
					if(tr.isSpan2Servers()) {
						
						// Creates a Hyperedge entry
//						e = new Hyperedge(tr.getTr_id());
//						
//						// Determine the nh_i values for each Virtual node
//						addNHValues(cluster, wb, tr, e);						
//						
//						// Determine the contribution, i.e., C_e and ndt_e for each Transaction
//						c_e = (double) ndt_e / total_tr_contribution;						
//						e.c_e = c_e;
//						e.ndt_e = ndt_e;
//						e.vertices.addAll(tr.getTr_dataSet());
//						e.s_i.addAll(tr.getTr_serverSet());
//						pq.add(e);
						
					}
				} else {  // Non-DT -- second set of CN
					//addNHValues(cluster, tr, e);
				}
			} // end -- for() -- transaction
		} // end -- for() -- transaction type
	
		
		// Testing		
		for(int i = 0; i < pq.size(); i++) {
			//System.out.println(pq.peek());
			calculateSG(pq.poll());
		}
	}
	
	@SuppressWarnings("unused")
	private void addNHValues(Cluster cluster, WorkloadBatch wb, Transaction tr, CompressedHEdge e) {
		
		for(Integer d_id : tr.getTr_dataSet()) {
			Data d = cluster.getData(d_id);
			
			int v_id = d.getData_vdata_id();
			int s_id = d.getData_server_id();
			
			// Add the corresponding v_id into V_i and V_cut
			v_cut.add(v_id);
			e.getV_i().add(v_id);
			e.getS_i().add(s_id);
		}
		
		// Figure out the sum of weights of the transactions incident on all the v_ids in e.v_i
		
		Set<Integer> trSet = new TreeSet<Integer>();
		
		for(Integer v_id : e.getV_i()) {
			int total_incidentTrWeight = 0;
			
			VirtualData v = cluster.getVdataSet().get(v_id);
			int s_id = v.getVdata_server_id();
			
//			for(Integer d_id : v.getVdata_set()) {
//				if(wb.getWrl_dataTransactionsInvolved().get(d_id) != null) {
//					//System.out.println("--> #tr = "+wb.getWrl_dataTransactionsInvolved().get(d_id).size());
//					trSet.addAll(wb.getWrl_dataTransactionsInvolved().get(d_id));
//				}
//			}
			
			//System.out.println("@@ size = "+trSet.size());
			for(Integer t_id : trSet) {
				Transaction t = wb.getTransaction(t_id);
				total_incidentTrWeight += (t.getTr_frequency()*t.getTr_temporal_weight());
				
				if(!e.getNh_i().containsKey(s_id)) {
					
					SortedMap<Integer, Integer> nhSet = new TreeMap<Integer, Integer>();
					nhSet.put(v_id, total_incidentTrWeight);								
					e.getNh_i().put(s_id, nhSet);
					
				} else {
					
					if(!e.getNh_i().get(s_id).containsKey(v_id)) {
						e.getNh_i().get(s_id).put(v_id, total_incidentTrWeight);
					} else {
						int weight = e.getNh_i().get(s_id).get(v_id);
						e.getNh_i().get(s_id).remove(v_id);
						e.getNh_i().get(s_id).put(v_id, (total_incidentTrWeight + weight));
					}
				}
			}
		}		
	}
	
	// Calculates the swapping gain
	public boolean calculateSG(CompressedHEdge e) {
		int SG1 = 0, SG2 = 0;
		int[] arr = new int[2];
		
		int i = 0;
		Iterator<Integer> itr = e.getS_i().iterator();
		while(itr.hasNext()) {
			arr[i] = itr.next(); 
			++i;
		}
		
		SG1 = 2 * e.getNdt_e() - e.getTotalNHi(arr[0]);
		SG2 = 2 * e.getNdt_e() - e.getTotalNHi(arr[1]);
		
		System.out.println("--> SG1 = "+SG1+" | s_id = "+arr[0]);
		System.out.println("--> SG2 = "+SG2+" | s_id = "+arr[1]);
		
		// Comparing swapping gain
		if(SG1 >= SG2 && SG1 > 0) {
			System.out.println(">> SG1 = "+SG1);
			
		} else if(SG1 >= SG2 && SG1 > 0) {
			System.out.println(">> SG2 = "+SG2);
			
		}
				
		return false;
	}
}