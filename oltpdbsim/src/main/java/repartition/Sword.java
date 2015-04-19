package main.java.repartition;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import main.java.cluster.Cluster;
import main.java.utils.graph.CompressedHEdge;
import main.java.utils.graph.CompressedVertex;
import main.java.utils.graph.SimpleHEdge;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class Sword {	
	
	// <v_id, <s_id, sum of weights of transactions incident of v_id>> -- <v_id, <s_id, nh>>
	public SortedMap<Integer, SortedMap<Integer, Integer>> dt_cnSet; 	
	public SortedMap<Integer, SortedMap<Integer, Integer>> ndt_cnSet;
	
	public static Set<SwordCHEdge> hCut; // Set of Compressed Hyperedges in the min-cut
	public static PriorityQueue<SwordCHEdge> pq; 	// Priority Queue of Compressed Hyperedges in min-cut
	
	public Set<Integer> pCut; // Union set of Partitions (i.e. Servers) covered by all Compressed Hyperedges in the min-cut
	public Set<Integer> sCut; // Union set of Partitions (i.e. Servers) covered by all Compressed Hyperedges in the min-cut

	// First set of candidate Virtual Nodes (i.e. Compressed Vertices) for migration
	public Set<SwordCVertex> vCut; // Union set of Virtual Nodes (i.e. Compressed Vertices) covered by all Hyperedges in the min-cut
	// Second set of candidate Virtual Nodes (i.e. Compressed Vertices) for migration
	public Set<SwordCVertex> VS; // Set of Virtual Nodes (i.e. Compressed Vertices) that are covered only by the Hyperedges that are not cut
	
	public Sword() {
		dt_cnSet = new TreeMap<Integer, SortedMap<Integer, Integer>>();
		ndt_cnSet = new TreeMap<Integer, SortedMap<Integer, Integer>>();
		
		hCut = new HashSet<SwordCHEdge>();
		pCut = new HashSet<Integer>();
		sCut = new HashSet<Integer>();
		
		vCut = new HashSet<SwordCVertex>();
		VS = new HashSet<SwordCVertex>();
	}
	
	// Populates a priority queue to keep the potential transactions 
	public static void populatePQ(Cluster cluster, WorkloadBatch wb) {		
		pq = new PriorityQueue<SwordCHEdge>(wb.hgr.getEdges().size(), SwordCHEdge.by_MIN_C_e());		
				
		double sum_ndt_e = 0.0;
		for(Entry<CompressedHEdge, Set<CompressedVertex>> ch : wb.hgr.getcHEdges().entrySet()) {			
			sum_ndt_e += ch.getKey().getWeight();
		}
		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> ch : wb.hgr.getcHEdges().entrySet()) {			
			double C_e = ch.getKey().getWeight()/sum_ndt_e;
			
			if(isCHEdgeDT(wb, ch.getKey())) {
				SwordCHEdge s_ch = new SwordCHEdge(ch.getKey().getId(), ch.getKey().getWeight(), C_e);
				hCut.add(s_ch);
				pq.add(s_ch);
			}
		}
		
		// Testing
		while(!pq.isEmpty()) {
			System.out.println(pq.poll().toString());
		}
	}
	
	public static boolean isCHEdgeDT(WorkloadBatch wb, CompressedHEdge ch) {
		boolean contains = false;
		
		for(Entry<Integer, SimpleHEdge> h : ch.getHESet().entrySet()) {
			Transaction tr = wb.getTransaction(h.getValue().getId());
				if(tr.isDt())
					return true;
				contains = true;
		}
		
		if(contains)
			return false;
		else
			return true;
	}
	
	
/*	public void init(Cluster cluster, WorkloadBatch wb) {
		
		pq = new PQ<SwordCHEdge>();
		calculateContribution(cluster, wb);
		
		// Testing		
		for(int i = 0; i < pq.size(); i++) {
			//System.out.println(pq.peek());
			calculateSG(wb, pq.poll());
		}
	}*/	
	
/*	public void calculateContribution(Cluster cluster, WorkloadBatch wb) {
		
		double sum_ndt_e = 0.0d;
				
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : wb.hgr.getcHEdges().entrySet()) {
			
			if(wb.hgr.isSpans2Server(cluster, wb, entry.getKey())) {
				sum_ndt_e += (double) entry.getKey().getWeight();
			} 
		}
		
		// new -- Calculate C_e (Contribution of each hyperedge e in H_cut towards total number of distributed transactions seen so far)
		double ndt_e = 0;
		double c_e = 0.0d;
		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : wb.hgr.getcHEdges().entrySet()) {
			
			CompressedHEdge ch = entry.getKey();
			
			if(wb.hgr.isSpans2Server(cluster, wb, entry.getKey())) {
				
				ndt_e = entry.getKey().getWeight();
				c_e = (double) ndt_e / sum_ndt_e;				
				 
				ch.setC_e(c_e);
				ch.setNdt_e(ndt_e);
				
				System.out.println(">> ndt_e = "+ndt_e+" | sum = "+sum_ndt_e);
				
				hCut.add(ch);
				pCut.addAll(wb.hgr.getIncidentPartitions(wb, ch));
				sCut.addAll(wb.hgr.getIncidentServers(wb, ch));
								
				Set<CompressedVertex> cvSet = new HashSet<CompressedVertex>();
				cvSet = wb.hgr.getIncidentCVertices(ch);
				
				vCut.addAll(cvSet);
				calculateNHValues(cluster, wb, cvSet);
					
				pq.add(ch);
				
			} else {
				ch.setC_e(0);
				ch.setNdt_e(0);
				
				// add the virtual nodes those are not in the cut 
				VS.addAll(wb.hgr.getcHEdges().get(ch));
			} 
		}				
	}
	
	private void calculateNHValues(Cluster cluster, WorkloadBatch wb, Set<CompressedVertex> cvSet) {
		
		for(CompressedVertex cv : cvSet) {
			int sum_nh = 0;
			
			for(CompressedHEdge ch :  wb.hgr.getcVertices().get(cv))
				sum_nh += ch.getWeight();			
			
			cv.setNh(sum_nh);
		}		
	}
	
	// Calculates the swapping gain
	public boolean calculateSG(WorkloadBatch wb, CompressedHEdge ch) {
		
		double SG1 = 0, SG2 = 0;
		int nh = 0;
		Map<Integer, Integer> serverNhSet = new HashMap<Integer, Integer>();
		
		for(CompressedVertex cv : wb.hgr.getcHEdges().get(ch)) {
			if(serverNhSet.containsKey(cv.getSid())) {
				nh = serverNhSet.get(cv.getSid());
				nh += cv.getNh();
				
				serverNhSet.remove(cv.getSid());
				serverNhSet.put(cv.getSid(), nh);
				
			} else {
				serverNhSet.put(cv.getSid(), cv.getNh());
			}
		}
				
		int[] arr = new int[2];
		int i = 0;
		System.out.println(">> "+serverNhSet.keySet().size());
		for(Integer s : serverNhSet.keySet()) {
			arr[i] = serverNhSet.get(s);
			++i;
		}
		
		SG1 = 2 * ch.getNdt_e() - arr[0];
		SG2 = 2 * ch.getNdt_e() - arr[1];
		
		System.out.println("--> SG1 = "+SG1+" | ndt_e"+ch.getNdt_e()+" | nh1 = "+arr[0]);
		System.out.println("--> SG2 = "+SG2+" | ndt_e"+ch.getNdt_e()+" | nh2 = "+arr[1]);
		
		// Comparing swapping gain
		if(SG1 >= SG2 && SG1 > 0) {
			System.out.println(">> SG1 = "+SG1);
			
		} else if(SG1 >= SG2 && SG1 > 0) {
			System.out.println(">> SG2 = "+SG2);
			
		}
				
		return false;
	}*/
}

class SwordCHEdge {
	int id;
	int ndt_e;
	double C_e;
	
	public SwordCHEdge(int id, int weight, double contribution) {
		this.id = id;
		this.ndt_e = weight;
		this.C_e = contribution;
	}
	
	// Ascending order
	static Comparator<SwordCHEdge> by_MIN_C_e() {
		return new Comparator<SwordCHEdge>() {
			@Override
			public int compare(SwordCHEdge ch1, SwordCHEdge ch2) {
				return Double.compare(ch1.C_e, ch2.C_e);
			}			
		};
	}
	
	@Override
	public String toString() {
		return (">> CH("+this.id+") | ndt_e("+this.ndt_e+") | C_e("+this.C_e+")");
	}
}

class SwordCVertex {
	int id;
	Set<Double> nhSet;
	
	public SwordCVertex(int id) {
		this.id = id;
		this.nhSet = new HashSet<Double>();
	}
	
}