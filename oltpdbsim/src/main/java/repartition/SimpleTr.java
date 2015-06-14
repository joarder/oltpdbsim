package main.java.repartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import java.util.Map.Entry;

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
	int min_data_mgr;
	double period;	
	double max_idt_gain; // Higher is better
	double max_lb_gain; // Lower is better
	double max_association_gain; // Higher is better
	HashMap<Integer, HashSet<Integer>> dataMap;
	HashMap<Integer, Double> associationMap;
	public List<MigrationPlan> migrationPlanList;
	public boolean isAssociated;
	boolean isProcessed;
	
	static double max_idt_reduction_improvement;
	static double max_lb_improvement;
	static double max_association_improvement;
	
	SimpleTr(int id, double period) {
		this.id = id;
		this.min_data_mgr = Integer.MAX_VALUE;
		this.period = period;
		this.max_idt_gain = 0;
		this.max_lb_gain = Integer.MAX_VALUE;
		this.dataMap = new HashMap<Integer, HashSet<Integer>>();
		this.migrationPlanList = new ArrayList<MigrationPlan>();
		this.associationMap = new HashMap<Integer, Double>();
		this.isAssociated = false;
		this.isProcessed = false;
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
	
	@SuppressWarnings("rawtypes")
	static TreeSet associationRank;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	void populateMigrationList(Cluster cluster, WorkloadBatch wb) {
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
		
		// Sorting migration list
		sortMigrationList();
		
		this.max_idt_gain = this.migrationPlanList.get(0).idt_gain_per_data_mgr;
		this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_data_mgr;
		this.min_data_mgr = this.migrationPlanList.get(0).req_data_mgr;
		
		// Testing
		/*System.out.println("-------------------------------------------------------------------------");
		System.out.println("Sorting based on combined ranking ...");
		System.out.println("--> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println("\t"+m.toString());
		}*/
	}
	
	// Calculate the similarity/association of each transaction and the derived clusters
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void populateAssociationList(Cluster cluster, WorkloadBatch wb, 
			HashMap<Integer, FCICluster> fci_clusters) {
		
		// Sets for preserving the ranks
		associationRank = new TreeSet();
		lbRank = new TreeSet();
		
		for(Entry<Integer, HashSet<Integer>> entry : this.dataMap.entrySet()) {
			HashMap<Integer, HashSet<Integer>> dataMap;
			HashSet<Integer> fromSet = new HashSet<Integer>();			
			int to = entry.getKey();
			int req_dmgr = 0;
			
			// Get the 'from' server set 
			for(int from : this.dataMap.keySet()) {
				if(from !=  to)
					fromSet.add(from);
			}
			
			dataMap = new HashMap<Integer, HashSet<Integer>>();
			// Get the tuple id from the 'from' server set
			for(int from : fromSet) {						
				req_dmgr += this.dataMap.get(from).size();
				dataMap.put(from, this.dataMap.get(from));
			}
			
			MigrationPlan m = new MigrationPlan(fromSet,to, dataMap, req_dmgr);
			this.migrationPlanList.add(m); // From Source Server
			
			m.association_gain_per_data_mgr = getAssociationGain(fci_clusters, entry, req_dmgr);						
			m.lb_gain_per_data_mgr = getLbGain(cluster, this, m);						
						
			associationRank.add(m.association_gain_per_data_mgr);
			lbRank.add(m.lb_gain_per_data_mgr);			
		} //end-for()
		
		// Get the maximum Association and Lb gains for this transaction for normalization purpose
		max_association_improvement = (double) associationRank.last();
		max_lb_improvement = (double) lbRank.last();
		
		// Sort the migration list based on the given weight
		sortMigrationList();
				
		this.max_association_gain = this.migrationPlanList.get(0).association_gain_per_data_mgr;
		this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_data_mgr;
		this.min_data_mgr = this.migrationPlanList.get(0).req_data_mgr;
				
		if(this.max_association_gain > 0)
			this.isAssociated = true;
		
		// Testing
		/*System.out.println("-------------------------------------------------------------------------");
		System.out.println("Sorting based on combined ranking ...");
		System.out.println("--> "+this.toString());
		for(MigrationPlan m : this.migrationPlanList) {
			System.out.println("\t"+m.toString());
		}*/
	}	

	// Returns the Idt gain per data movement 
	static double getIdtGain(WorkloadBatch wb, SimpleTr t, MigrationPlan m) {
		
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
		SimpleTr t = new SimpleTr(tr.getTr_id(), tr.getTr_period());
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
	static double getLbGain(Cluster cluster, SimpleTr t, MigrationPlan m) {		
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
	
	// Returns the association gain
	private double getAssociationGain(HashMap<Integer, FCICluster> fci_clusters, 
			Entry<Integer, HashSet<Integer>> entry, int req_dmgr) {
		
		double association = 0.0;
		
		if(fci_clusters.containsKey(entry.getKey())) {
			double C_i = fci_clusters.get(entry.getKey()).fci.size();
			double T_C_i = Sets.intersection(fci_clusters.get(entry.getKey()).fci, entry.getValue()).size();		
			association = (double) (fci_clusters.get(entry.getKey()).weight * (T_C_i/C_i));
			association /= req_dmgr; 
			this.associationMap.put(entry.getKey(), association);
		}
		
		return association;
	}
	
	// Sorting migration lists
	private void sortMigrationList() {
		if(Global.idt_priority > Global.lb_priority) {
			// Always take the one with maximum difference
			// Sort in descending order
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){				
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {
					
					double m1_left = 0.0;
					double m2_left = 0.0;
					
					if(Global.associative) {
						m1_left = (m1.association_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
						m2_left = (m2.association_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
					} else {
						m1_left = (m1.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
						m2_left = (m2.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
					}
					
					m1.combined_weight = Math.abs(m1_left - (m1.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority);
					m2.combined_weight = Math.abs(m2_left - (m2.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority);					
		            
					return ((m1.combined_weight > m2.combined_weight) ? -1 : 
						(m1.combined_weight < m2.combined_weight) ? 1 : 0);
				}
			});			
		} else {
			
			/*If all are negative then take the largest one (sort in descending order)
			If all are positive then take the smallest one (sort in ascending order)
			If there is a mix of positives and negatives then take the one closest to zero*/
			 			
			// Sort in ascending order
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){				
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {
					
					double m1_left = 0.0;
					double m2_left = 0.0;
					
					if(Global.associative) {
						m1_left = (m1.association_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
						m2_left = (m2.association_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
					} else {
						m1_left = (m1.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
						m2_left = (m2.idt_gain_per_data_mgr/max_idt_reduction_improvement)*Global.idt_priority;
					}
					
					m1.combined_weight = m1_left - (m1.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority;
					m2.combined_weight = m2_left - (m2.lb_gain_per_data_mgr/max_lb_improvement)*Global.lb_priority;
					
					return ((m1.combined_weight > m2.combined_weight) ? -1 : 
						(m1.combined_weight < m2.combined_weight) ? 1 : 0);
				}
			});
		}	
	}
	
	// Descending order
	static Comparator<SimpleTr> by_MAX_IDT_REDUCTION_IMPROVEMENT() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t1.max_idt_gain > t2.max_idt_gain) ? -1 : 
					(t1.max_idt_gain < t2.max_idt_gain) ? 1 : 0);
			}			
		};
	}
	
	// Ascending order
	static Comparator<SimpleTr> by_MAX_LB_IMPROVEMENT() {
		return new Comparator<SimpleTr>() {
			@Override
			public int compare(SimpleTr t1, SimpleTr t2) {
				return ((t2.max_lb_gain > t1.max_lb_gain) ? -1 : 
					(t2.max_lb_gain < t1.max_lb_gain) ? 1 : 0);
			}			
		};
	}
	
	// Descending order
	static Comparator<SimpleTr> by_MAX_ASSOCIATION_IMPROVEMENT() {
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
		return (">> T"+this.id+": Min required data migrations ("+this.min_data_mgr+") "
				+ "| Max Idt gain ("+this.max_idt_gain+") "
				+ "| Max Lb gain ("+this.max_lb_gain+") "
				+ "| Max Association gain ("+this.max_association_gain+") "
						+ "| "+this.dataMap);
	}
}