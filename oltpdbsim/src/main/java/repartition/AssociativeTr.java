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
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.collect.Sets;

public class AssociativeTr implements Comparable<AssociativeTr> {
	int id;
	int min_dmgr;
	double period;	
	double max_association_gain; // Higher is better
	double max_lb_gain; // Lower is better		
	
	HashMap<Integer, HashSet<Integer>> dataMap;
	HashMap<Integer, Double> associationMap;
	public boolean isAssociated;
	
	public List<MigrationPlan> migrationPlanList;	
	boolean isProcessed;
	
	AssociativeTr(int id, double period) {
		this.id = id;
		this.min_dmgr = Integer.MAX_VALUE;
		this.period = period;
		this.max_association_gain = 0;
		this.max_lb_gain = Integer.MAX_VALUE;
		this.dataMap = new HashMap<Integer, HashSet<Integer>>();
		this.associationMap = new HashMap<Integer, Double>();
		this.migrationPlanList = new ArrayList<MigrationPlan>();
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
	static TreeSet associationRank;
	@SuppressWarnings("rawtypes")
	static TreeSet lbRank;
	
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
			
			if(Global.idt_priority == 1.0)
				m.association_gain_per_dmgr = getAssociationGain(fci_clusters, entry, req_dmgr);
			else if((1 - Global.idt_priority) == 1.0) {
				m.association_gain_per_dmgr = getAssociationGain(fci_clusters, entry, req_dmgr); // Only to find the associative transactions
				m.lb_gain_per_dmgr = getLbGain(cluster, this, m);
			} else {
				m.association_gain_per_dmgr = getAssociationGain(fci_clusters, entry, req_dmgr);						
				m.lb_gain_per_dmgr = getLbGain(cluster, this, m);						
			}
			
			associationRank.add(m.association_gain_per_dmgr);
			lbRank.add(m.lb_gain_per_dmgr);			
		} //end-for()
		
		// Setting the maximum Association and Lb gains for this transaction
		this.max_association_gain = (double) associationRank.last();
		this.max_lb_gain = (double) lbRank.first();
		
		if(this.max_association_gain > 0)
			this.isAssociated = true;		
		
		// Sorting
		if(Global.idt_priority == 1.0) {
			// Sorting in descending order by the Association
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {				
					return (((double)m1.association_gain_per_dmgr > (double)m2.association_gain_per_dmgr) ? -1 : 
						((double)m1.association_gain_per_dmgr < (double)m2.association_gain_per_dmgr) ? 1 : 0);				
				}
			});
			
			this.max_association_gain = this.migrationPlanList.get(0).association_gain_per_dmgr;			
						
			// Testing
			// After sorting
			/*System.out.println("-------------------------------------------------------------------------");
			System.out.println("Sorting based on association gain per data migration ...");
			System.out.println("--> "+this.toString());
			for(MigrationPlan m : this.migrationPlanList) {
				System.out.println("\t"+m.toString());
			}*/

		} else if((1 - Global.idt_priority) == 1.0) {
			// Sorting in ascending order by Lb
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {				
					return (((double)m2.lb_gain_per_dmgr > (double)m1.lb_gain_per_dmgr) ? -1 : 
						((double)m2.lb_gain_per_dmgr < (double)m1.lb_gain_per_dmgr) ? 1 : 0);				
				}
			});
			
			this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_dmgr;
			
			// Testing
			// After sorting
			/*System.out.println("-------------------------------------------------------------------------");
			System.out.println("Sorting based on load balance improvement per data migration ...");
			System.out.println("--> "+this.toString());
			for(MigrationPlan m : this.migrationPlanList) {
				System.out.println("\t"+m.toString());
			}*/
			
		} else {
			// Sort the array list by the value of combined rank (descending order)
			Collections.sort(this.migrationPlanList, new Comparator<MigrationPlan>(){
				@Override
				public int compare(MigrationPlan m1, MigrationPlan m2) {
					int association_rank1 = ((TreeSet) associationRank).headSet(m1.association_gain_per_dmgr).size();
		            int association_rank2 = ((TreeSet) associationRank).headSet(m2.association_gain_per_dmgr).size();
		            
		            int lb_rank1 = ((TreeSet) lbRank).tailSet(m1.lb_gain_per_dmgr).size();
		            int lb_rank2 = ((TreeSet) lbRank).tailSet(m2.lb_gain_per_dmgr).size();
		            
		            m1.combined_rank = association_rank1*Global.idt_priority + lb_rank1*(1 - Global.idt_priority);
		            m2.combined_rank = association_rank2*Global.idt_priority + lb_rank2*(1 - Global.idt_priority);
		            
					return (((double)m1.combined_rank > (double)m2.combined_rank) ? -1 : 
						((double)m1.combined_rank < (double)m2.combined_rank) ? 1 : 0);				
				}
			});
			
			this.max_association_gain = this.migrationPlanList.get(0).association_gain_per_dmgr;
			this.max_lb_gain = this.migrationPlanList.get(0).lb_gain_per_dmgr;
			
			// Testing
			/*System.out.println("-------------------------------------------------------------------------");
			System.out.println("Sorting based on combined ranking ...");
			System.out.println("--> "+this.toString());
			for(MigrationPlan m : this.migrationPlanList) {
				System.out.println("\t"+m.toString());
			}*/
		}
		
		this.min_dmgr = this.migrationPlanList.get(0).req_dmgr;
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
	
	// Returns the lb gain
	private double getLbGain(Cluster cluster, AssociativeTr t, MigrationPlan m) {		
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

		return (new_server_data.getVariance()/m.req_dmgr);
	}
	
	// Descending order
	static Comparator<AssociativeTr> by_MAX_ASSOCIATION_IMPROVEMENT() {
		return new Comparator<AssociativeTr>() {
			@Override
			public int compare(AssociativeTr t1, AssociativeTr t2) {
				return ((t1.max_association_gain > t2.max_association_gain) ? -1 : 
					(t1.max_association_gain < t2.max_association_gain) ? 1 : 0);
			}			
		};
	}
	
	// Ascending order
	static Comparator<AssociativeTr> by_MAX_LB_IMPROVEMENT() {
		return new Comparator<AssociativeTr>() {
			@Override
			public int compare(AssociativeTr t1, AssociativeTr t2) {
				return ((t2.max_lb_gain > t1.max_lb_gain) ? -1 : 
					(t2.max_lb_gain < t1.max_lb_gain) ? 1 : 0);
			}			
		};
	}
	
	@Override
	public int compareTo(AssociativeTr t) {					
		return (((int)this.id > (int)t.id) ? -1 : 
			((int)this.id < (int)t.id) ? 1 : 0);
	}
	
	@Override
	public String toString() {
		return (">> T"+this.id+": Max association gain ("+this.max_association_gain+") "
				+ "| Max Lb gain ("+this.max_lb_gain+") "
					+ "| "+this.associationMap
						+ "| "+this.dataMap);
	}
}