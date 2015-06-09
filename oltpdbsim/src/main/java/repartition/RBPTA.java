/**
 * @author Joarder Kamal
 * New algorithm implementation | Jan 14, 15
 */

package main.java.repartition;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.entry.Global;
import main.java.utils.IntPair;
import main.java.utils.Matrix;
import main.java.utils.MatrixElement;
import main.java.utils.Utility;
import main.java.workload.Transaction;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

/*
 * Repartition Based on Partition-level Transactional Association (RBPTA)
 */

public class RBPTA {

	public static Matrix association;

	public static void init(Cluster cluster) {		
		int M = cluster.getPartitions().size()+1;
		int N = M;				
		association = Utility.createMatrix(M, N);
	}

	public static void updateAssociation(Cluster cluster, Transaction tr) {
		//System.out.println(">> "+tr.toString());
		// Based on: https://code.google.com/p/combinatoricslib/
		// Get all the pairs of combinations of Partition accessed by this Transaction
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = 
				Factory.createVector(tr.getTr_partitionSet().keySet());

		// Create a simple combination generator to generate 2-combinations of the initial vector
		Generator<Integer> gen = Factory.createSimpleCombinationGenerator(initialVector, 2);
		
		// Print all possible combinations
		for (ICombinatoricsVector<Integer> combination : gen) {			
		
			int p_a_id = combination.getValue(0);
			int p_b_id = combination.getValue(1);
			
			int p_a_trData = tr.getTr_partitionSet().get(p_a_id).size();
			int p_b_trData = tr.getTr_partitionSet().get(p_b_id).size();
			
			double entropy = getEntropy(p_a_trData, p_b_trData);			
			entropy /= 2;
			double association_value = (1/tr.getTr_period()) * (p_a_trData + p_b_trData) * entropy;
			//double association_value = entropy;
			//System.out.println("\t>> P"+p_a_id+"-P"+p_b_id);
			//System.out.println("\t\t--> New entropy = "+entropy);
						
			if(p_b_id > p_a_id) {
				double old_association = association.getMatrix()[p_b_id][p_a_id].getValue();
				//System.out.println("\t\t--> Old association = "+old_association);
				
				double new_association = old_association * Global.expAvgWt 
						+ association_value * (1 - Global.expAvgWt);
				
				association.getMatrix()[p_b_id][p_a_id].setValue(new_association);
				//System.out.println("\t\t--> New association = "+new_association);
				
			} else {
				double old_association = association.getMatrix()[p_a_id][p_b_id].getValue();
				//System.out.println("\t\t--> Old association = "+old_association);
				
				double new_association = old_association * Global.expAvgWt 
						+ association_value * (1 - Global.expAvgWt);
								
				association.getMatrix()[p_a_id][p_b_id].setValue(new_association);
				//System.out.println("\t\t--> New association = "+new_association);
			}			
		}
		
		// Verification
		//System.out.println("====================================================================");
		//association.print();
		//this.migrationDecision(cluster);
	}
	
	private static double getEntropy(int p_a_trData, int p_b_trData) {
		double entropy = 0.0d;
		
		double p_total = p_a_trData + p_b_trData;
		entropy = (-(p_a_trData/p_total) * (Math.log(p_a_trData/p_total)/Math.log(2))
						 -(p_b_trData/p_total) * (Math.log(p_b_trData/p_total)/Math.log(2)));
		
		return entropy;
	}
	
	
	@SuppressWarnings("rawtypes")
	static TreeSet sgGainRank;
	@SuppressWarnings("rawtypes")
	static TreeSet lbGainRank;		
	
	// Migration decision
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public static IntPair migrationDecision(Cluster cluster) {
		
		ArrayList<SwappingCandidate> swappingCandidates = new ArrayList<SwappingCandidate>();			
		Set<Integer> serverSet = new TreeSet<Integer>();
		
		for(Server s : cluster.getServers())
			serverSet.add(s.getServer_id());
		
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(serverSet);

		// Create a simple combination generator to generate 2-combinations of the initial vector
		Generator<Integer> gen = Factory.createSimpleCombinationGenerator(initialVector, 2);
		
		Server s_a = null;
		Server s_b = null;
		
		// Print all possible combinations
		for (ICombinatoricsVector<Integer> combination : gen) {	
			
			s_a = cluster.getServer(combination.getValue(0));
			s_b = cluster.getServer(combination.getValue(1));						
			
			for(int p_a_id : s_a.getServer_partitions()) {
				Partition p_a = cluster.getPartition(p_a_id);
				
				// Sets for preserving the ranks
				sgGainRank = new TreeSet();
				lbGainRank = new TreeSet();
				
				for(int p_b_id : s_b.getServer_partitions()) {					
					
					Partition p_b = cluster.getPartition(p_b_id);
					MatrixElement me = null;
					
					if(p_a_id > p_b_id)
						me = association.getMatrix()[p_a_id][p_b_id];
					else
						me = association.getMatrix()[p_b_id][p_a_id];
						
					SwappingCandidate sc = new SwappingCandidate(me.getId());
					
					sc.p_pair.x = me.getRow_pos();
					sc.p_pair.y = me.getCol_pos();
					
					sc.s_pair.x = s_a.getServer_id();
					sc.s_pair.y = s_b.getServer_id();
					
					sc.swapping_gain = getSwappingGain(cluster, p_a, p_b);					
					sc.lb_gain = getLbGain(cluster, sc);
					
					sgGainRank.add(sc.swapping_gain);
					lbGainRank.add(sc.lb_gain);
					
					swappingCandidates.add(sc);
					
				} // end-for()
			} // end-for()
		} // end-while()
		
		// Testing
		/*System.out.println("--> List of potential swapping candidates: ");
		for(SwappingCandidate s : swappingCandidates) {
			System.out.println("\t\t"+s.toString());
		}*/		
		
		// Chose the best swapping candidate
		if(swappingCandidates.size() != 0) {		
			// Sorting
			if(Global.idt_priority == 1.0) {
				// Sort the array list by swapping gain in decending order
				Collections.sort(swappingCandidates, new Comparator<SwappingCandidate>(){
					@Override
					public int compare(SwappingCandidate sc1, SwappingCandidate sc2) {					
						return (((double)sc1.swapping_gain > (double)sc2.swapping_gain) ? -1 : 
		        			((double)sc1.swapping_gain < (double)sc2.swapping_gain) ? 1 : 0);
					}
				});
				
			} else if(Global.lb_priority == 1.0) {
				// Sort the array list by lb gain in ascending order
				Collections.sort(swappingCandidates, new Comparator<SwappingCandidate>(){
					@Override
					public int compare(SwappingCandidate sc1, SwappingCandidate sc2) {					
						return (((double)sc2.lb_gain > (int)sc1.lb_gain) ? -1 : 
		        			((double)sc2.lb_gain < (int)sc2.lb_gain) ? 1 : 0);
					}
				});
					
			} else {
				// Sort the array list by the value of combined rank (descending order)
				Collections.sort(swappingCandidates, new Comparator<SwappingCandidate>(){				
					@Override
					public int compare(SwappingCandidate sc1, SwappingCandidate sc2) {
						int sg_rank1 = ((TreeSet) sgGainRank).headSet(sc1.swapping_gain).size();
			            int sg_rank2 = ((TreeSet) sgGainRank).headSet(sc2.swapping_gain).size();
			            
			            int lb_rank1 = ((TreeSet) lbGainRank).tailSet(sc1.lb_gain).size();
			            int lb_rank2 = ((TreeSet) lbGainRank).tailSet(sc2.lb_gain).size();
			            
			            sc1.combined_rank = sg_rank1 * Global.idt_priority + lb_rank1 * Global.lb_priority;
			            sc2.combined_rank = sg_rank2 * Global.idt_priority + lb_rank2 * Global.lb_priority;
			            
						return (((double)sc1.combined_rank > (double)sc2.combined_rank) ? -1 : 
							((double)sc1.combined_rank < (double)sc2.combined_rank) ? 1 : 0);				
					}
				});
			}
								

			/*Global.LOGGER.info("----------------------------------------------");
			Global.LOGGER.info("Sorted list of potential swapping pairs:");			
			for(SwappingCandidate sc : swappingCandidates)
				Global.LOGGER.info("--"+sc.toString());*/
						
			// Select the target swapping candidate
			SwappingCandidate target_candidate = swappingCandidates.get(0);			
			
			//Global.LOGGER.info("----------------------------------------------");
			//Global.LOGGER.info("Selected swapping pair: "+target_candidate.toString());
					
			return (new IntPair(target_candidate.p_pair.x, target_candidate.p_pair.y));
		
		} else {
			return null;
		}
	}
	
	// Returns the actual gain for a swapping Pi/Si with Pj/Sj 
	private static double getSwappingGain(Cluster cluster, Partition p_i, Partition p_j) {
		
		int p_i_id = p_i.getPartition_id();
		int p_j_id = p_j.getPartition_id();
		
		Server s_i = cluster.getServer(p_i.getPartition_serverId());
		Server s_j = cluster.getServer(p_j.getPartition_serverId());
		
		double value = 0.0d;
		double gain = 0.0d;
		double loss = 0.0d;
		
		// Get the gain for Pi-->Sj
		for(int p_id : s_j.getServer_partitions()) {
			if(p_i_id != p_id) {
				if(p_i_id > p_id)			
					value = association.getMatrix()[p_i_id][p_id].getValue();
				else
					value = association.getMatrix()[p_id][p_i_id].getValue();
						
				gain += value;
			}
		}
		
		// Get the gain for Pj-->Si
		for(int p_id : s_i.getServer_partitions()) {
			if(p_j_id != p_id) {
				if(p_j_id > p_id)			
					value = association.getMatrix()[p_j_id][p_id].getValue();
				else
					value = association.getMatrix()[p_id][p_j_id].getValue();
						
				gain += value;
			}
		}
		
		// Get the loss for Pi<--Si
		for(int p_id : s_i.getServer_partitions()) {
			if(p_i_id != p_id) {
				if(p_i_id > p_id)		
					value = association.getMatrix()[p_i_id][p_id].getValue();
				else
					value = association.getMatrix()[p_id][p_i_id].getValue();			
			
				loss += value;
			}
		}

		// Get the loss for Pj<--Sj
		for(int p_id : s_j.getServer_partitions()) {
			if(p_j_id != p_id) {
				if(p_j_id > p_id)		
					value = association.getMatrix()[p_j_id][p_id].getValue();
				else
					value = association.getMatrix()[p_id][p_j_id].getValue();
						
				loss += value;
			}
		}
				
		// Returns the actual gain
		return (gain - loss);
	}
	
	// Returns the difference of data volume of two partitions as a distance
	private static double getLbGain(Cluster cluster, SwappingCandidate sc) {
		
		DescriptiveStatistics sc_partition_data = new DescriptiveStatistics();
		
		for(Partition p : cluster.getPartitions())
			if(sc.p_pair.x == p.getPartition_id() || sc.p_pair.y == p.getPartition_id())
				sc_partition_data.addValue(p.getPartition_dataSet().size());

		return sc_partition_data.getVariance();
	}	
}

// A specialised Element Class
class SwappingCandidate {
	int id;
	IntPair p_pair;
	IntPair s_pair;
	double swapping_gain;
	double lb_gain;
	double combined_rank;
	
	public SwappingCandidate(int id) {
		this.setId(id);
		this.p_pair = new IntPair(-1, -1);
		this.s_pair = new IntPair(-1, -1);
		this.swapping_gain = 0.0;
		this.lb_gain = 0.0;
		this.combined_rank = 0.0;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	@Override
	public boolean equals(Object object) {
		if (!(object instanceof MatrixElement)) {
			return false;
		}
		
		MatrixElement e = (MatrixElement) object;
		return (this.getId() == e.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.id;
		return result;
	}
	
	@Override
	public String toString() {		
		return ("P"+this.p_pair+"/S"+this.s_pair
				+" [Swapping Gain("+this.swapping_gain+") | "+" Lb gain("+this.lb_gain+") | Combined rank("+this.combined_rank+"]");	
	}
}