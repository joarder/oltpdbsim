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

package main.java.repartition;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;

import main.java.cluster.Cluster;
import main.java.cluster.CompressedData;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.utils.IntPair;
import main.java.utils.graph.CompressedHEdge;
import main.java.utils.graph.CompressedVertex;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.WorkloadBatch;

public class Sword {	
	
	// <v_id, <s_id, sum of weights of transactions incident of v_id>> -- <v_id, <s_id, nh>>
	public static HashMap<Integer, HashMap<Integer, Integer>> dt_cnSet; 	
	public static HashMap<Integer, HashMap<Integer, Integer>> ndt_cnSet;
	
	public static Set<SwordCHEdge> hCut; // Set of Compressed Hyperedges in the Cut
	public static PriorityQueue<SwordCHEdge> pq; 	// Priority Queue of Compressed Hyperedges in Cut
	
	public static Set<Integer> pCut; // Union set of Partitions covered by all Compressed Hyperedges in the Cut
	public static Set<Integer> sCut; // Union set of Servers covered by all Compressed Hyperedges in the Cut

	// First set of candidate Compressed Vertices for migration
	public static Map<Integer, HashMap<Integer, SwordCVertex>> vCut; // Union set of Compressed Vertices covered by all Hyperedges in the Cut
	// Second set of candidate Compressed Vertices for migration
	public static Map<Integer, HashMap<Integer, SwordCVertex>> VS; // Set of Compressed Vertices that are covered only by the Hyperedges that are not Cut
	
	static ArrayList<IntPair> candidatePairList;
	static Set<Integer> swapped;
	
	public static void init() {
		dt_cnSet = new HashMap<Integer, HashMap<Integer, Integer>>();
		ndt_cnSet = new HashMap<Integer, HashMap<Integer, Integer>>();
		
		hCut = new HashSet<SwordCHEdge>();
		pCut = new HashSet<Integer>();
		sCut = new HashSet<Integer>();
		
		vCut = new HashMap<Integer, HashMap<Integer, SwordCVertex>>();
		VS = new HashMap<Integer, HashMap<Integer, SwordCVertex>>();
		
		candidatePairList = new ArrayList<IntPair>();
		swapped = new HashSet<Integer>();
	}
	
	// Populates a priority queue to keep the potential transactions 
	public static void populatePQ(Cluster cluster, WorkloadBatch wb) {
		init();
		
		pq = new PriorityQueue<SwordCHEdge>(wb.hgr.getEdges().size(), SwordCHEdge.by_MAX_C_e());		
		
		// Calculate the sum of the weights of the compressed hyperedges in the cut
		double sum_ndt_e = 0.0;
		for(Entry<CompressedHEdge, Set<CompressedVertex>> ch_entry : wb.hgr.getCHEdgeMap().entrySet()) {
			if(isCHEdgeInCut(wb, ch_entry.getKey()))
				sum_ndt_e += ch_entry.getKey().getWeight();
		}
		
		// Calculate contribution of each compressed hyperedge toward total number of distributed transactions seen so far
		for(Entry<CompressedHEdge, Set<CompressedVertex>> ch_entry : wb.hgr.getCHEdgeMap().entrySet()) {
			
			if(isCHEdgeInCut(wb, ch_entry.getKey())) {
				
				if(isCHEdgeSpansTwoServers(wb, ch_entry.getKey())) {
					
					double C_e = ch_entry.getKey().getWeight()/sum_ndt_e;				
					SwordCHEdge sch = new SwordCHEdge(ch_entry.getKey().getId(), 
							ch_entry.getKey().getWeight(), C_e);
					
					hCut.add(sch);
					pq.add(sch);				
				
					// Compressed Vertices in the Cut for the the selected compressed hyperedges
					for(CompressedVertex cv : ch_entry.getValue()) {
						
						SwordCVertex scv = new SwordCVertex(cv.getId(), cv.getWeight(), 
								cv.getPartition_id(), cv.getServer_id(), cv.getVSet().values());
						
						// Adding scv into sch 
						if(sch.sCVertexSet.containsKey(scv.server_id)) {
							sch.sCVertexSet.get(scv.server_id).put(scv.id, scv);
						} else {
							HashMap<Integer, SwordCVertex> scvSet = new HashMap<Integer, SwordCVertex>();
							scvSet.put(scv.id, scv);
							sch.sCVertexSet.put(scv.server_id, scvSet);
						}
												
						// Adding servers, partitions, and vertices
						sch.serverSet.add(cv.getServer_id());
						sch.partitionSet.add(cv.getPartition_id());
						
						// Adding scv into vCut
						if(vCut.containsKey(cv.getServer_id())) {
							vCut.get(cv.getServer_id()).put(scv.id, scv);
						} else {
							HashMap<Integer, SwordCVertex> scvSet = new HashMap<Integer, SwordCVertex>();
							scvSet.put(scv.id, scv);
							vCut.put(cv.getServer_id(), scvSet);
						}
						
						pCut.add(cv.getPartition_id());
						sCut.add(cv.getServer_id());						
					}
				}
			} else {
				// Compressed hyperedges not in the Cut
				for(CompressedVertex cv : ch_entry.getValue()) {
					SwordCVertex scv = new SwordCVertex(cv.getId(), cv.getWeight(), 
							cv.getPartition_id(), cv.getServer_id(), cv.getVSet().values());
					
					// Compressed vertices not in the Cut
					if(vCut.containsKey(cv.getServer_id())) {
						if(!vCut.get(cv.getServer_id()).containsKey(scv.id)) {
							if(VS.containsKey(cv.getServer_id())) {
								VS.get(cv.getServer_id()).put(scv.id, scv);	
							} else {
								HashMap<Integer, SwordCVertex> scvSet = new HashMap<Integer, SwordCVertex>();
								scvSet.put(scv.id, scv);
								VS.put(scv.server_id, scvSet);
							}
						}
					}
				}
			}
		}
		
		// Keep only the movable vertices in VS which are not in the Cut
		Set<SwordCVertex> toBeRemoved = new HashSet<SwordCVertex>();
		for(Entry<Integer, HashMap<Integer, SwordCVertex>> entry : VS.entrySet()) {
			for(Entry<Integer, SwordCVertex> scv_entry : entry.getValue().entrySet())			
				if(vCut.get(entry.getKey()).containsKey(scv_entry.getKey()))
					toBeRemoved.add(scv_entry.getValue());			
		}

		// Remove the non-movable compressed vertices from VS
		for(SwordCVertex scv : toBeRemoved) 
			VS.get(scv.server_id).remove(scv);
		
		// Adding more compressed data nodes to VS to produce more swapping candidates
		for(Entry<Integer, CompressedData> cd_entry : cluster.getCDataSet().entrySet()) {
			
			CompressedData cd = cd_entry.getValue();
			
			SwordCVertex scv = new SwordCVertex(cd.getCData_id(), 0, 
					cd.getVdata_partition_id(), cd.getVdata_server_id(), null);
			
			if(vCut.containsKey(scv.server_id) && VS.containsKey(scv.server_id)) {
				if(!vCut.get(scv.server_id).containsKey(scv) && !VS.get(scv.server_id).containsKey(scv)) {
					scv.vertexSet = new HashSet<Integer>(cd.getCData_set());
					VS.get(scv.server_id).put(scv.id, scv);
				}
			}
		}
		
		// Testing
		/*System.out.println("@ HEdges = "+wb.hgr.getEdgeCount());
		System.out.println("@ Vertices = "+wb.hgr.getVertexCount());
		System.out.println("@ Total Data = "+Global.global_dataCount);
		System.out.println("@ CHEdges = "+wb.hgr.getcHEdges().size());
		System.out.println("@ CVertices = "+wb.hgr.getcVertices().size());
		System.out.println("@ hCut size = "+hCut.size());
		System.out.println("@ vCut size = "+vCut.size()+" | "+vCut.get(1).size()+","+vCut.get(2).size()+","+vCut.get(3).size()+","+vCut.get(4).size());
		System.out.println("@ VS size = "+VS.size()+" | "+VS.get(1).size()+","+VS.get(2).size()+","+VS.get(3).size()+","+VS.get(4).size());
		System.out.println("@ pCut size = "+pCut.size());
		System.out.println("@ sCut size = "+sCut.size());
		System.out.println("@ PQ size = "+pq.size());*/

	}
		
	// Migration
	public static void swapCandidatePair(Cluster cluster, WorkloadBatch wb, SwordCHEdge sch) {
		//System.out.println("--> "+sch.toString());
		ArrayList<Integer> serverSet = new ArrayList<Integer>(sch.serverSet);
		
		int s1 = serverSet.get(0);
		int s2 = serverSet.get(1);
		
		double nh1 = getSumNHij(sch, s1);
		double nh2 = getSumNHij(sch, s2);
		
		double SG1 = 2*sch.ndt_e - nh1;
		double SG2 = 2*sch.ndt_e - nh2;
		
		//System.out.println("\t--> SG1 = "+SG1+" | ndt_e = "+sch.ndt_e+" | nh1 = "+nh1);
		//System.out.println("\t--> SG2 = "+SG2+" | ndt_e = "+sch.ndt_e+" | nh2 = "+nh2);
		
		// Comparing swapping gain and select a possible swapping candidate pair
		if(SG1 >= SG2 && SG1 > 0) { // Migrate s1 --> s2
			//System.out.println("\t>> SG1 = "+SG1);
			getSwappingPair(sch, s1, s2);		
					
		} else if(SG2 >= SG1 && SG2 > 0) { // Migrate s2 --> s1
			//System.out.println("\t>> SG2 = "+SG2);
			getSwappingPair(sch, s2, s1);
		}
		
		// Actual data migration
		migrate(cluster);
	}
		
	// Calculate swapping gain
	private static double getSumNHij(SwordCHEdge ch, int server_id) {		
		double sum_nh_ij = 0.0d;
		for(Entry<Integer, SwordCVertex> scv_entry : ch.sCVertexSet.get(server_id).entrySet())
			sum_nh_ij += scv_entry.getValue().nh_ij;		
		
		return sum_nh_ij;
	}
	
	// Selecting swapping pair
	private static void getSwappingPair(SwordCHEdge sch, int src_server_id, int dst_server_id) {
		
		for(Entry<Integer, SwordCVertex> scv_entry : sch.sCVertexSet.get(src_server_id).entrySet()) {
			int vs_id = getVSSwappingCandidate(dst_server_id);
			
			if(vs_id != -1)
				candidatePairList.add(new IntPair(scv_entry.getValue().id, vs_id));
		}
	}		
	
	// Returns a swapping candidate from VS
	private static int getVSSwappingCandidate(int dst_server_id) {
		
		if(VS.containsKey(dst_server_id)) {
			ArrayList<Integer> temp = new ArrayList<Integer>(VS.get(dst_server_id).keySet());
			int rand_id = Global.rand.nextInt(temp.size());
			int _id = VS.get(dst_server_id).get(temp.get(rand_id)).id;
			
			SwordCVertex scv = VS.get(dst_server_id).get(_id);
			int scv_id = scv.id;
	
			if(!swapped.contains(scv_id)) {	
				swapped.add(scv_id);			
				VS.get(dst_server_id).remove(scv_id);
				
				if(VS.get(dst_server_id).isEmpty())
					VS.remove(dst_server_id);
				
				return scv_id;			
			} else
				return -1;
		}
		
		return -1;
	}
	
	// Data migration
	private static void migrate(Cluster cluster) {
		// Testing
		/*System.out.println(swappingPairList.size()+" | "+swappingPairList);		
		System.out.println(">> "+VS.size());
		for(Entry<Integer, HashMap<Integer, SwordCVertex>> entry : VS.entrySet()) {
			System.out.println("\t"+entry.getValue().size());
			
			for(Entry<Integer, SwordCVertex> entry1 : entry.getValue().entrySet())
				System.out.println("\t\t "+entry1.toString());
		}*/
		
		for(IntPair p : candidatePairList) {			
			CompressedData cd1 = cluster.getCDataSet().get(p.x);
			CompressedData cd2 = cluster.getCDataSet().get(p.y);

			// First compressed data
			int dst_server_id1 = cd2.getVdata_server_id();
			int dst_partition_id1 = cd2.getVdata_partition_id();
			
			for(int d_id : cd1.getCData_set()) {
				Data data = cluster.getData(d_id);
				DataMigration.migrateSingleData(cluster, data, dst_server_id1, dst_partition_id1);
			}

			// Second compressed data
			int dst_server_id2 = cd1.getVdata_server_id();
			int dst_partition_id2 = cd1.getVdata_partition_id();
			
			for(int d_id : cd2.getCData_set()) {
				Data data = cluster.getData(d_id);
				DataMigration.migrateSingleData(cluster, data, dst_server_id2, dst_partition_id2);
			}
			
			// Updating first compressed data
			cd1.setCData_partition_id(dst_partition_id1);
	    	cd1.setCData_server_id(dst_server_id1);
	    	
			// Updating second compressed data 
			cd2.setCData_partition_id(dst_partition_id2);
	    	cd2.setCData_server_id(dst_server_id2);
		}
	}
	
	// Checks and returns true if a compressed hyperedge is in the Cut i.e. distributed
	public static boolean isCHEdgeInCut(WorkloadBatch wb, CompressedHEdge ch) {
		Set<Integer> serverSet = new HashSet<Integer>();
		
		for(SimpleVertex cv : wb.hgr.getCHEdgeMap().get(ch))
			serverSet.add(cv.getServer_id());
		
		if(serverSet.size() > 1)
			return true;
				
		return false;		
	}
	
	// Checks and returns true if a compressed hyperedge is in the Cut i.e. distributed
	public static boolean isCHEdgeSpansTwoServers(WorkloadBatch wb, CompressedHEdge ch) {
		Set<Integer> serverSet = new HashSet<Integer>();
		
		for(SimpleVertex cv : wb.hgr.getCHEdgeMap().get(ch))
			serverSet.add(cv.getServer_id());
		
		if(serverSet.size() == 2)
			return true;
				
		return false;		
	}
}

class SwordCHEdge  implements Comparable<SwordCHEdge> {
	int id;
	double ndt_e; // Weight of the hyperedge e in the Cut
	double C_e; // Contribution in DT
	Set<Integer> serverSet;
	Set<Integer> partitionSet;
	Map<Integer, Map<Integer, SwordCVertex>> sCVertexSet;
	
	public SwordCHEdge(int id, double weight, double contribution) {
		this.id = id;
		this.ndt_e = weight;
		this.C_e = contribution;
		this.serverSet = new HashSet<Integer>();
		this.partitionSet = new HashSet<Integer>();
		this.sCVertexSet = new HashMap<Integer, Map<Integer, SwordCVertex>>();
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
		return ("CH("+this.id+") | ndt_e("+this.ndt_e+") | C_e("+this.C_e+") "
				+ "| S"+this.serverSet+" | P"+this.partitionSet+" | "+this.sCVertexSet);
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
		
		if(vSet != null) {
			this.vertexSet = new HashSet<Integer>();
			for(SimpleVertex v : vSet)
				vertexSet.add(v.getId());
		}
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
		return ("CV("+this.id+") | nh_ij("+this.nh_ij+") | P("+this.partition_id+")/S("+this.server_id+") | "+this.vertexSet);
	}
}