package main.java.repartition;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.VirtualData;
import main.java.utils.graph.CompressedHEdge;
import main.java.utils.graph.CompressedVertex;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.utils.queue.PQ;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class Sword {	
	
	// <v_id, <s_id, sum of weights of transactions incident of v_id>> -- <v_id, <s_id, nh>>
	public SortedMap<Integer, SortedMap<Integer, Integer>> dt_cnSet; 	
	public SortedMap<Integer, SortedMap<Integer, Integer>> ndt_cnSet;
	
	public PQ<CompressedHEdge> pq; 	// Priority Queue of Hyperedges
	public Set<SimpleHEdge> hCut; // Set of Hyperedges in the min-cut
	public Set<Integer> pCut; // Union set of Partitions covered by all Hyperedges in the min-cut
	public Set<Integer> sCut; // Union set of Partitions covered by all Hyperedges in the min-cut

	// First set of candidate Virtual Nodes for migration
	public Set<CompressedVertex> vCut; // Union set of Virtual Nodes covered by all Hyperedges in the min-cut
	// Second set of candidate Virtual Nodes for migration
	public Set<CompressedVertex> VS; // Set of Virtual Nodes that are covered only by the Hyperedges that are not cut
	
	public Sword() {
		dt_cnSet = new TreeMap<Integer, SortedMap<Integer, Integer>>();
		ndt_cnSet = new TreeMap<Integer, SortedMap<Integer, Integer>>();
		
		hCut = new HashSet<SimpleHEdge>();
		pCut = new HashSet<Integer>();
		sCut = new HashSet<Integer>();
		
		vCut = new HashSet<CompressedVertex>();
		VS = new HashSet<CompressedVertex>();
	}
	
	public void init(Cluster cluster, WorkloadBatch wb) {
		
		pq = new PQ<CompressedHEdge>(wb.hgr.getEdgeCount(), Collections.reverseOrder());
		this.calculateContribution(cluster, wb);
		
		// Testing		
		for(int i = 0; i < pq.size(); i++) {
			System.out.println(pq.peek());
			//calculateSG(pq.poll());
		}
	}	
	
	public void calculateContribution(Cluster cluster, WorkloadBatch wb) {
		
		double sum_ndt_e = 0.0d;
				
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : wb.hgr.getcHEdges().entrySet()) {
			
			if(wb.hgr.isCHEdgeDT(wb, entry.getKey()) && entry.getValue().size() >= 2) {
				sum_ndt_e += (double) entry.getKey().getWeight();
			} 
		}
		
		// new -- Calculate C_e (Contribution of each hyperedge e in H_cut towards total number of distributed transactions seen so far)
		double ndt_e = 0.0d;
		double c_e = 0.0d;
		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : wb.hgr.getcHEdges().entrySet()) {
			
			CompressedHEdge ch = entry.getKey();
			
			if(wb.hgr.isCHEdgeDT(wb, entry.getKey()) && entry.getValue().size() >= 2) {
				
				ndt_e = (double) entry.getKey().getWeight();
				c_e = ndt_e / sum_ndt_e;				
				 
				ch.setC_e(c_e);
				ch.setNdt_e(ndt_e);
				 
				hCut.add(ch);
				pCut.addAll(wb.hgr.getIncidentPartitions(wb, ch));
				sCut.addAll(wb.hgr.getIncidentServers(wb, ch));
								
				Set<CompressedVertex> cvSet = new HashSet<CompressedVertex>();
				cvSet = wb.hgr.getIncidentCVertices(ch);
				
				vCut.addAll(cvSet);
				calculateNHValues(cluster, wb, cvSet);
					
				pq.add(ch);
					
			} else {
				ch.setC_e(0.0d);
				ch.setNdt_e(0.0d);
				
				// add the virtual nodes those are not in the cut 
				VS.addAll(wb.hgr.getcHEdges().get(ch));
			} 
		}
	}
	
	private void calculateNHValues(Cluster cluster, WorkloadBatch wb, Set<CompressedVertex> cvSet) {
		
		for(CompressedVertex cv : cvSet) {
			for(Entry<Integer, SimpleVertex> v : cv.getVSet().entrySet()) {
				Data d =  cluster.getData(v.getValue().getId());
				int p_id = d.getData_partition_id(); 
			}
		}		
	}
	
	// Calculates the swapping gain
//	public boolean calculateSG(CompressedHEdge ch) {
//		int SG1 = 0, SG2 = 0;
//		int[] arr = new int[2];
//		
//		int i = 0;
//		Iterator<Integer> itr = ch.getS_i().iterator();
//		while(itr.hasNext()) {
//			arr[i] = itr.next(); 
//			++i;
//		}
//		
//		SG1 = 2 * ch.getNdt_e() - ch.getTotalNHi(arr[0]);
//		SG2 = 2 * ch.getNdt_e() - ch.getTotalNHi(arr[1]);
//		
//		System.out.println("--> SG1 = "+SG1+" | s_id = "+arr[0]);
//		System.out.println("--> SG2 = "+SG2+" | s_id = "+arr[1]);
//		
//		// Comparing swapping gain
//		if(SG1 >= SG2 && SG1 > 0) {
//			System.out.println(">> SG1 = "+SG1);
//			
//		} else if(SG1 >= SG2 && SG1 > 0) {
//			System.out.println(">> SG2 = "+SG2);
//			
//		}
//				
//		return false;
//	}
}