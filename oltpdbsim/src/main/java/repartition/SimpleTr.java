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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Server;
import main.java.dsm.FCICluster;
import main.java.entry.Global;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import com.google.common.collect.Sets;

public class SimpleTr implements Comparable<SimpleTr> {
	int id;		
	double period;
	
	int min_data_mgr;
	int max_span_reduction;
	
	double max_span_reduction_gain; // Higher is better
	double max_idt_gain; // Higher is better
	double max_lb_gain; // Lower is better
	double max_association_gain; // Higher is better
	double max_combined_weight; // Higher is better
	
	boolean isProcessed;
	
	// For normalisation purpose only
	static double max_span_reduction_value;
	static double max_idt_reduction_value;
	static double max_lb_value;
	static double max_association_value;
	
	static TreeSet<Double> spanReductionGainRank;
	static TreeSet<Double> idtGainRank;
	static TreeSet<Double> lbGainRank;
	static TreeSet<Double> associationRank;
	
	HashSet<Integer> dataSet;
	HashMap<Integer, HashSet<Integer>> serverDataSet;
	public List<MigrationPlan> migrationPlanList;
	public boolean isAssociated;	
	
	SimpleTr(int id, double period) {
		this.id = id;
		this.period = period;
		
		this.min_data_mgr = Integer.MAX_VALUE;
		this.max_span_reduction = 0;
		
		this.max_span_reduction_gain = 0.0d;
		this.max_idt_gain = 0.0d;
		this.max_lb_gain = Integer.MAX_VALUE;
		this.max_association_gain = 0.0d;
		this.max_combined_weight = 0.0d;
				
		this.isProcessed = false;
		
		this.dataSet = new HashSet<Integer>();
		this.serverDataSet = new HashMap<Integer, HashSet<Integer>>();
		this.migrationPlanList = new ArrayList<MigrationPlan>();
		this.isAssociated = false;
	}
	
	void populateServerSet(Cluster cluster, Transaction tr) {
		for(int d_id : tr.getTr_dataSet()) {
			Data d = cluster.getData(d_id);			
			int s_id = d.getData_server_id();
			
			if(this.serverDataSet.containsKey(s_id)) {
				this.serverDataSet.get(s_id).add(d.getData_id());
			} else {
				HashSet<Integer> dataSet = new HashSet<Integer>();
				dataSet.add(d.getData_id());
				this.serverDataSet.put(s_id, dataSet);
			}
			
			// Also populate the data set
			this.dataSet.add(d.getData_id());
		}
	}
	
	// Used for heuristic based algorithms
	void populateMigrationList(Cluster cluster, WorkloadBatch wb) {
		// Based on: https://code.google.com/p/combinatoricslib/
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(this.serverDataSet.keySet());

		// Create a simple permutation generator to generate N-permutations of the initial vector
		Generator<Integer> permutationGen = Factory.createPermutationWithRepetitionGenerator(initialVector, this.serverDataSet.size());		
		HashMap<HashSet<Integer>, Integer> uniqueFromSet = new HashMap<HashSet<Integer>, Integer>();
		
		// Get all possible N-permutations		
		HashMap<Integer, HashSet<Integer>> dataMap;
		
		idtGainRank = new TreeSet<Double>();
		lbGainRank = new TreeSet<Double>();
		
		for (ICombinatoricsVector<Integer> permutations : permutationGen) {
			HashSet<Integer> fromSet = new HashSet<Integer>();		
			
			for(int i = 0; i < permutations.getSize()-1; i++)
				fromSet.add(permutations.getValue(i));			
						
			int to = permutations.getValue(permutations.getSize() - 1);
			
			if(!fromSet.contains(to)) {
				if(!uniqueFromSet.containsKey(fromSet)
						|| (uniqueFromSet.containsKey(fromSet) && !uniqueFromSet.get(fromSet).equals(to))) {
					
					if(fromSet.size() <= Global.spanReduce) {
						
						dataMap = new HashMap<Integer, HashSet<Integer>>();
						int req_data_mgr = 0;
						
						for(int from : fromSet) {						
							req_data_mgr += this.serverDataSet.get(from).size();
							dataMap.put(from, this.serverDataSet.get(from));
						}										
						
						MigrationPlan m = new MigrationPlan(fromSet, to, dataMap, req_data_mgr);
						this.migrationPlanList.add(m); // From Source Server
						
						m.idt_gain_per_data_mgr = getIdtGain(wb, this, m);						
						m.lb_gain_per_data_mgr = getLbGain(cluster, this, m);					
						
						idtGainRank.add(m.idt_gain_per_data_mgr);
						lbGainRank.add(m.lb_gain_per_data_mgr);	
						
						if(fromSet.size() > 1)
							uniqueFromSet.put(fromSet, to);
					}
				} //end-if()
			} //end-if()	
		} // end-for(

		// Get the maximum Idt and Lb gains for this transaction for normalization purpose
		max_idt_reduction_value = idtGainRank.last();
		max_lb_value = lbGainRank.last();
		
		// Sorting migration list
		sortMigrationPlanList();
		
		this.max_idt_gain = this.migrationPlanList.get(0).idt_gain_per_data_mgr;
		this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_data_mgr;
		this.min_data_mgr = this.migrationPlanList.get(0).req_data_mgr;	
		this.max_combined_weight = this.migrationPlanList.get(0).combined_weight;
		
		// Testing
		/*System.out.println("-------------------------------------------------------------------------");
		System.out.println("Sorting based on combined ranking ...");
		System.out.println("--> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println("\t"+m.toString());
		}*/
	}
	
	// Only used for heuristic based span reduction algorithm
/*	void populateMigrationList(Cluster cluster, WorkloadBatch wb, int span_reduce) {
		// Based on: https://code.google.com/p/combinatoricslib/
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(this.serverDataSet.keySet());

		// Create a simple permutation generator to generate N-permutations of the initial vector
		Generator<Integer> permutationGen = Factory.createPermutationWithRepetitionGenerator(initialVector, this.serverDataSet.size());		
		HashMap<HashSet<Integer>, Integer> uniqueFromSet = new HashMap<HashSet<Integer>, Integer>();
		
		// Get all possible N-permutations		
		HashMap<Integer, HashSet<Integer>> dataMap;	
		
		spanReductionGainRank = new TreeSet<Double>();
		
		for (ICombinatoricsVector<Integer> permutations : permutationGen) {
			HashSet<Integer> fromSet = new HashSet<Integer>();		
			
			for(int i = 0; i < permutations.getSize()-1; i++)
				fromSet.add(permutations.getValue(i));			
						
			int to = permutations.getValue(permutations.getSize() - 1);
			
			if(!fromSet.contains(to)) {
				
				if(!uniqueFromSet.containsKey(fromSet)
						|| (uniqueFromSet.containsKey(fromSet) && !uniqueFromSet.get(fromSet).equals(to))) {
										
					if(fromSet.size() <= Global.spanReduce) {
						
						dataMap = new HashMap<Integer, HashSet<Integer>>();
						int req_data_mgr = 0;
						
						for(int from : fromSet) {						
							req_data_mgr += this.serverDataSet.get(from).size();
							dataMap.put(from, this.serverDataSet.get(from));
						}
					
						MigrationPlan m = new MigrationPlan(fromSet, to, dataMap, req_data_mgr);
						this.migrationPlanList.add(m); // From Source Server
						
						m.span_reduction_per_data_mgr = getSpanReductionGain(wb, this, m);						
						spanReductionGainRank.add(m.span_reduction_per_data_mgr);
						
						if(fromSet.size() > 1)
							uniqueFromSet.put(fromSet, to);
					
					} //end inner-if()
				} //end-if()
			} //end-if()	
		} // end-for(

		// Get the maximum span reduction gain for this transaction for normalization purpose
		max_span_reduction_value = spanReductionGainRank.last();
		
		// Sorting migration list
		sortMigrationPlanList();				
		
		this.min_data_mgr = this.migrationPlanList.get(0).req_data_mgr;
		this.max_span_reduction = this.migrationPlanList.get(0).serverDataSet.size();
		this.max_span_reduction_gain = this.migrationPlanList.get(0).span_reduction_per_data_mgr;		
		
		// Testing
		System.out.println("-------------------------------------------------------------------------");
		System.out.println("Sorting based on combined ranking ...");
		System.out.println("--> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println("\t"+m.toString());
		}
	}*/	
	
	// Calculate the similarity/association of each transaction and the derived clusters
	public void populateAssociationList(Cluster cluster, WorkloadBatch wb, 
			HashMap<Integer, FCICluster> fci_clusters) {
		
		// Sets for preserving the ranks
		associationRank = new TreeSet<Double>();
		lbGainRank = new TreeSet<Double>();
		
		for(Entry<Integer, HashSet<Integer>> server : this.serverDataSet.entrySet()) {
			HashMap<Integer, HashSet<Integer>> dataMap;
			HashSet<Integer> fromSet = new HashSet<Integer>();			
			int to = server.getKey();
			int req_dmgr = 0;
			
			// Get the 'from' server set 
			for(int from : this.serverDataSet.keySet()) {
				if(from !=  to)
					fromSet.add(from);
			}
			
			dataMap = new HashMap<Integer, HashSet<Integer>>();
			// Get the tuple id from the 'from' server set
			for(int from : fromSet) {						
				req_dmgr += this.serverDataSet.get(from).size();
				dataMap.put(from, this.serverDataSet.get(from));
			}
			
			MigrationPlan m = new MigrationPlan(fromSet,to, dataMap, req_dmgr);
			this.migrationPlanList.add(m); // From Source Server
			
			m.association_gain_per_data_mgr = getAssociationGain(fci_clusters, server, m);			
			m.lb_gain_per_data_mgr = getLbGain(cluster, this, m);						
						
			associationRank.add(m.association_gain_per_data_mgr);
			lbGainRank.add(m.lb_gain_per_data_mgr);			
		} //end-for()
		
		// Get the maximum Association and Lb gains for this transaction for normalization purpose
		max_association_value = (double) associationRank.last();
		max_lb_value = (double) lbGainRank.last();
		
		// Sort the migration list based on the given weight
		sortMigrationPlanList();
				
		this.max_association_gain = this.migrationPlanList.get(0).association_gain_per_data_mgr;
		this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_data_mgr;
		this.min_data_mgr = this.migrationPlanList.get(0).req_data_mgr;
		this.max_combined_weight = this.migrationPlanList.get(0).combined_weight;
			
		if(this.max_association_gain > 0.0d || this.max_lb_gain < 0.0d)
			this.isAssociated = true;
				
		// Testing
		/*System.out.println("-------------------------------------------------------------------------");
		System.out.println("Sorting based on combined ranking ...");
		System.out.println("--> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println("\t"+m.toString());
		}*/
	}
	
	// Returns the span reduction gain per data migration
	/*static double getSpanReductionGain(WorkloadBatch wb, SimpleTr t, MigrationPlan m) {
		int original_span = t.serverDataSet.size();
		int expected_span = t.serverDataSet.size() - m.fromSet.size();
		
		int expected_span_reduction = original_span - expected_span;		
		return ((double)expected_span_reduction/m.req_data_mgr);
	}*/

	// Returns the expected Idt gain per data migration 
	static double getIdtGain(WorkloadBatch wb, SimpleTr t, MigrationPlan m) {
					
		int span_reduction = m.fromSet.size();
		int incident_span_reduction = 0;			
		
		for(Entry<Integer, HashSet<Integer>> entry : t.serverDataSet.entrySet()) {
			
			HashSet<Integer> unique_trSet = new HashSet<Integer>();
			
			for(Integer d_id : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d_id);
				
				for(SimpleHEdge  h : wb.hgr.getIncidentEdges(v)) {
					Transaction incident_tr = wb.getTransaction(h.getId());
					
					if(incident_tr.getTr_id() != t.id) {
						if(!unique_trSet.contains(incident_tr.getTr_id())) 
							incident_span_reduction += getIncidentSpanReduction(incident_tr, m);						
											
						unique_trSet.add(incident_tr.getTr_id());
					}
				}			
			}
		}		
						
		return ((double)(span_reduction + incident_span_reduction)/m.req_data_mgr); // Expected Net Improvement (ENI)			
	}
	
	// Returns the net span improvement for an incident transaction t for a migration plan m
	static int getIncidentSpanReduction(Transaction j, MigrationPlan m) {
		// Construct A -- for target transaction i
		Set<Integer> delta_i_A = new HashSet<Integer>();
		for(Integer s : m.fromSet)
			delta_i_A.addAll(m.serverDataSet.get(s));				

		// Construct B -- for incident transaction j
		Set<Integer> delta_j_A = new HashSet<Integer>();		
		for(Integer s : m.fromSet)
			if(j.getTr_serverSet().containsKey(s))
				delta_j_A.addAll(j.getTr_serverSet().get(s));				
		
		// Set difference, delta = delta_j_A \ delta_i_A
		Set<Integer> delta = new HashSet<Integer>(delta_j_A);
		delta.removeAll(delta_i_A);
		
		HashSet<Integer> psi_delta = new HashSet<Integer>();
		for(Entry<Integer, HashSet<Integer>> entry : j.getTr_serverSet().entrySet())
			if(!Collections.disjoint(delta, entry.getValue())) // Returns true if the two specified collections have no elements in common.
				psi_delta.add(entry.getKey());
		
		// Calculate net span improvement for incident transaction t
		int gain = m.fromSet.size() - psi_delta.size();
		
		if(!j.getTr_serverSet().containsKey(m.to))
			gain -= 1;
		
		return gain;
	}
	
	// Returns the expected load balance variance gain per data migration
	static double getLbGain(Cluster cluster, SimpleTr t, MigrationPlan m) {	
		
		// Before migration
		DescriptiveStatistics current_server_data = new DescriptiveStatistics();		
		for(Server s : cluster.getServers())
			if(t.serverDataSet.containsKey(s.getServer_id()))
				current_server_data.addValue(s.getServer_total_data());
		
		// After migration		
		DescriptiveStatistics expected_server_data = new DescriptiveStatistics();		
		for(Server s : cluster.getServers()) {
			if(t.serverDataSet.containsKey(s.getServer_id())) {
				
				if(m.fromSet.contains(s.getServer_id())) {
					int data_count = s.getServer_total_data() - t.serverDataSet.get(s.getServer_id()).size();
					expected_server_data.addValue(data_count);
					
				} else if(m.to == s.getServer_id()) {
					int data_count = s.getServer_total_data() + t.serverDataSet.get(s.getServer_id()).size();
					expected_server_data.addValue(data_count);
				}
			}
		}
		
		double net_gain = current_server_data.getVariance() - expected_server_data.getVariance(); 		
		return ((double)net_gain/m.req_data_mgr);
	}	
	
	// Returns the expected association gain per data migration
	private double getAssociationGain(HashMap<Integer, FCICluster> fci_clusters, 
			Entry<Integer, HashSet<Integer>> server, MigrationPlan m) {
		
		// For a particular server
		FCICluster c_i = fci_clusters.get(server.getKey());
		double expected_association_gain = 0.0;
			
		if(c_i != null) {
			int C_i = c_i.fci.size();
			int T_C_i = Sets.intersection(c_i.fci, this.dataSet).size();
			
			double association_value = (double)T_C_i/C_i;
			expected_association_gain = (double)c_i.weight * association_value;		
		}
		
		return ((double)expected_association_gain/m.req_data_mgr);
	}	
	
	// Sorting migration plans
	private void sortMigrationPlanList() {			
		// Sort in descending order
		// Higher the better
		Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){				
			@Override
			public int compare(MigrationPlan m1, MigrationPlan m2) {
				
				double m1_left = 0.0d;
				double m2_left = 0.0d;
				double m1_right = 0.0d;
				double m2_right = 0.0d;
				
				if(Global.associative) {
					m1_left = (m1.association_gain_per_data_mgr/max_association_value) * Global.idt_priority;
					m2_left = (m2.association_gain_per_data_mgr/max_association_value) * Global.idt_priority;
				} else {
					m1_left = (m1.idt_gain_per_data_mgr/max_idt_reduction_value) * Global.idt_priority;
					m2_left = (m2.idt_gain_per_data_mgr/max_idt_reduction_value) * Global.idt_priority;
				}
				
				m1_right = (m1.lb_gain_per_data_mgr/max_lb_value) * Global.lb_priority; 
				m2_right = (m2.lb_gain_per_data_mgr/max_lb_value) * Global.lb_priority;
				
				m1.combined_weight = m1_left - m1_right;
				m2.combined_weight = m2_left - m2_right;					
	            
				return ((m1.combined_weight > m2.combined_weight) ? -1 : 
					(m1.combined_weight < m2.combined_weight) ? 1 : 0);
			}
		});	
	}
	
	// Descending order
	static Comparator<SimpleTr> by_MAX_COMBINED_WEIGHT() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t1.max_combined_weight > t2.max_combined_weight) ? -1 : 
					(t1.max_combined_weight < t2.max_combined_weight) ? 1 : 0);
			}			
		};
	}	
	
	// Descending order
	/*static Comparator<SimpleTr> by_MAX_SPAN_REDUCTION_GAIN() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t1.max_span_reduction_gain > t2.max_span_reduction_gain) ? -1 : 
					(t1.max_span_reduction_gain < t2.max_span_reduction_gain) ? 1 : 0);
			}			
		};
	}	*/
	
	// Descending order
	static Comparator<SimpleTr> by_MAX_IDT_REDUCTION_GAIN() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t1.max_idt_gain > t2.max_idt_gain) ? -1 : 
					(t1.max_idt_gain < t2.max_idt_gain) ? 1 : 0);
			}			
		};
	}
	
	// Ascending order
	static Comparator<SimpleTr> by_MAX_LB_GAIN() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t2.max_lb_gain > t1.max_lb_gain) ? -1 : 
					(t2.max_lb_gain < t1.max_lb_gain) ? 1 : 0);
			}			
		};
	}
	
	// Descending order
	static Comparator<SimpleTr> by_MAX_ASSOCIATION_GAIN() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t1.max_association_gain > t2.max_association_gain) ? -1 : 
					(t1.max_association_gain < t2.max_association_gain) ? 1 : 0);
			}			
		};
	}
		
	// Ascending order
	static Comparator<SimpleTr> by_MIN_DATA_MIGRATIONS() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t2.min_data_mgr > t1.min_data_mgr) ? -1 : 
					(t2.min_data_mgr < t1.min_data_mgr) ? 1 : 0);
			}			
		};
	}
	
	@Override
	public int compareTo(SimpleTr t) {					
		return (((int)this.id > (int)t.id) ? -1 : 
			((int)this.id < (int)t.id) ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return (">> T"+this.id+": Min data migrations require ("+this.min_data_mgr+") "
				+ "| Max span reduction gain ("+this.max_span_reduction_gain+") "
					+ "| Max Idt gain ("+this.max_idt_gain+") "
					+ "| Max Lb gain ("+this.max_lb_gain+") "
						+ "| Max Association gain ("+this.max_association_gain+") "
							+ "| "+this.serverDataSet
								+  "|A-"+this.isAssociated
								 + "|P-"+this.isProcessed);
	}
}