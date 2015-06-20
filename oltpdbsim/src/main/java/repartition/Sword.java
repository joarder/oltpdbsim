package main.java.repartition;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import main.java.cluster.Cluster;
import main.java.entry.Global;
import main.java.utils.IntPair;
import main.java.utils.graph.CompressedHEdge;
import main.java.utils.graph.CompressedVertex;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.WorkloadBatch;

public class Sword {	
	
	// <v_id, <s_id, sum of weights of transactions incident of v_id>> -- <v_id, <s_id, nh>>
	public static SortedMap<Integer, SortedMap<Integer, Integer>> dt_cnSet; 	
	public static SortedMap<Integer, SortedMap<Integer, Integer>> ndt_cnSet;
	
	public static Set<SwordCHEdge> hCut; // Set of Compressed Hyperedges in the Cut
	public static PriorityQueue<SwordCHEdge> pq; 	// Priority Queue of Compressed Hyperedges in Cut
	
	public static Set<Integer> pCut; // Union set of Partitions covered by all Compressed Hyperedges in the Cut
	public static Set<Integer> sCut; // Union set of Servers covered by all Compressed Hyperedges in the Cut

	// First set of candidate Compressed Vertices for migration
	public static Set<SwordCVertex> vCut; // Union set of Compressed Vertices covered by all Hyperedges in the Cut
	// Second set of candidate Compressed Vertices for migration
	public static Set<SwordCVertex> VS; // Set of Compressed Vertices that are covered only by the Hyperedges that are not Cut
	
	public static void init() {
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
		init();
		
		pq = new PriorityQueue<SwordCHEdge>(wb.hgr.getEdges().size(), SwordCHEdge.by_MAX_C_e());		
		
		// Calculate the sum of the weights of the compressed hyperedges in the cut
		double sum_ndt_e = 0.0;
		for(Entry<CompressedHEdge, Set<CompressedVertex>> ch_entry : wb.hgr.getcHEdges().entrySet()) {
			if(isCHEdgeInCut(wb, ch_entry.getKey()))
				sum_ndt_e += ch_entry.getKey().getWeight();
		}
		
		// Calculate contribution of each compressed hyperedge toward total number of distributed transactions seen so far
		for(Entry<CompressedHEdge, Set<CompressedVertex>> ch_entry : wb.hgr.getcHEdges().entrySet()) {
			if(isCHEdgeInCut(wb, ch_entry.getKey())) {				
				if(isCHEdgeSpansTwoServers(wb, ch_entry.getKey())) {				
					double C_e = ch_entry.getKey().getWeight()/sum_ndt_e;				
					SwordCHEdge s_ch = new SwordCHEdge(ch_entry.getKey().getId(), ch_entry.getKey().getWeight(), C_e);
					
					hCut.add(s_ch);
					pq.add(s_ch);				
				
					// Compressed Vertices in the Cut for the the selected compressed hyperedges
					for(CompressedVertex cv : ch_entry.getValue()) {
						vCut.add(new SwordCVertex(cv.getId(), cv.getWeight(), 
								cv.getPartition_id(), cv.getServer_id(), cv.getVSet().values()));
						
						pCut.add(cv.getPartition_id());
						sCut.add(cv.getServer_id());
						
						// Adding servers, partitions, and vertices
						s_ch.server_Set.add(cv.getServer_id());
						s_ch.partition_Set.add(cv.getPartition_id());						
					}
				}
			} else {
				// Compressed hyperedges not in the Cut
				for(CompressedVertex cv : ch_entry.getValue()) {
					SwordCVertex scv = new SwordCVertex(cv.getId(), cv.getWeight(), 
							cv.getPartition_id(), cv.getServer_id(), cv.getVSet().values());
					
					if(!vCut.contains(scv))
						// Compressed vertices not in the Cut
						VS.add(scv);
				}
			}
		}
		
		// Keep only the movable vertices in VS which are not in the Cut
		Set<SwordCVertex> toBeRemoved = new HashSet<SwordCVertex>();
		for(SwordCVertex scv : VS) {
			if(vCut.contains(scv))
				toBeRemoved.add(scv);
		}

		// Remove the non-movable compressed vertices from VS
		for(SwordCVertex scv : toBeRemoved) 
			VS.remove(scv);
		
		// Testing
		System.out.println("@ HEdges = "+wb.hgr.getEdgeCount());
		System.out.println("@ Vertices = "+wb.hgr.getVertexCount());
		System.out.println("@ Total Data = "+Global.global_dataCount);
		System.out.println("@ CHEdges = "+wb.hgr.getcHEdges().size());
		System.out.println("@ CVertices = "+wb.hgr.getcVertices().size());
		System.out.println("@ hCut size = "+hCut.size());
		System.out.println("@ vCut size = "+vCut.size());
		System.out.println("@ VS size = "+VS.size()+" | "+VS);
		System.out.println("@ pCut size = "+pCut.size());
		System.out.println("@ sCut size = "+sCut.size());
		System.out.println("@ PQ size = "+pq.size());

	}
	
	public static SwappingPair getSwappingPair(SwordCHEdge ch) {
		
		
		
		
		double SG1 = getSwappingGain(ch);
		double SG2 = getSwappingGain(ch);
		
		System.out.println("--> SG1 = "+SG1+" | ndt_e"+ch.ndt_e+" | nh1 = ");
		System.out.println("--> SG2 = "+SG2+" | ndt_e"+ch.ndt_e+" | nh2 = ");
		
		// Comparing swapping gain
		if(SG1 >= SG2 && SG1 > 0) {
			System.out.println(">> SG1 = "+SG1);
			
		} else if(SG1 >= SG2 && SG1 > 0) {
			System.out.println(">> SG2 = "+SG2);
			
		}
		
		return new SwappingPair();
	}
	
	
	// Calculate swapping gain
	private static double getSwappingGain(SwordCHEdge ch) {
		
		return 0.0;
	}
	
	// Checks and returns true if a compressed hyperedge is in the Cut i.e. distributed
	public static boolean isCHEdgeInCut(WorkloadBatch wb, CompressedHEdge ch) {
		Set<Integer> serverSet = new HashSet<Integer>();
		
		for(SimpleVertex cv : wb.hgr.getcHEdges().get(ch))
			serverSet.add(cv.getServer_id());
		
		if(serverSet.size() > 1)
			return true;
				
		return false;		
	}
	
	// Checks and returns true if a compressed hyperedge is in the Cut i.e. distributed
	public static boolean isCHEdgeSpansTwoServers(WorkloadBatch wb, CompressedHEdge ch) {
		Set<Integer> serverSet = new HashSet<Integer>();
		
		for(SimpleVertex cv : wb.hgr.getcHEdges().get(ch))
			serverSet.add(cv.getServer_id());
		
		if(serverSet.size() == 2)
			return true;
				
		return false;		
	}
}

class SwordCHEdge  implements Comparable<SwordCHEdge> {
	int id;
	int ndt_e; // Weight of the hyperedge e in the Cut
	double C_e; // Contribution in DT
	Set<Integer> server_Set;
	Set<Integer> partition_Set;
	
	public SwordCHEdge(int id, int weight, double contribution) {
		this.id = id;
		this.ndt_e = weight;
		this.C_e = contribution;
		this.server_Set = new HashSet<Integer>();
		this.partition_Set = new HashSet<Integer>();
	}
	
	// Descending order
	static Comparator<SwordCHEdge> by_MAX_C_e() {
		return new Comparator<SwordCHEdge>() {
			@Override
			public int compare(SwordCHEdge ch1, SwordCHEdge ch2) {
				return Double.compare(ch2.C_e, ch1.C_e);
			}			
		};
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SwordCHEdge other = (SwordCHEdge) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
	@Override
	public int compareTo(SwordCHEdge sCHE) {					
		return ((this.id > sCHE.id) ? -1 : (this.id < sCHE.id) ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return (">> CH("+this.id+") | ndt_e("+this.ndt_e+") | C_e("+this.C_e+") | S"+this.server_Set+" | P"+this.partition_Set);
	}
}

class SwordCVertex implements Comparable<SwordCVertex> {
	int id;
	int nh_ij; //nh_i_j be the sum of the weights of hyperedges incident on node v_i in partition p_j
	int partition_id;
	int server_id;
	Set<Integer> vertexSet;
	
	public SwordCVertex(int id, int weight, int pid, int sid, Collection<SimpleVertex> vSet) {
		this.id = id;
		this.nh_ij = weight;
		this.partition_id = pid;
		this.server_id = sid;
		
		this.vertexSet = new HashSet<Integer>();
		for(SimpleVertex v : vSet)
			vertexSet.add(v.getId());				
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SwordCVertex other = (SwordCVertex) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
	@Override
	public int compareTo(SwordCVertex sCV) {					
		return ((this.id > sCV.id) ? -1 : (this.id < sCV.id) ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return (">> CV("+this.id+") | nh_ij("+this.nh_ij+") | P("+this.partition_id+")/S("+this.server_id+") | "+this.vertexSet);
	}
}

class SwappingPair {
	IntPair candidatePair;
	double gain;
	
	SwappingPair() {
		this.gain = 0.0;
	}
}