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

import org.paukov.combinatorics.Factory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;


public class Association {

	public static Matrix association;

	public void init(Cluster cluster) {		
		int M = cluster.getPartitions().size()+1;
		int N = M;				
		association = Utility.createMatrix(M, N);
	}

	public static void updateAssociation(Cluster cluster, Transaction tr) {
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
			entropy = (1/tr.getTr_period()) * entropy;			
			entropy /= 2;
						
			if(p_b_id > p_a_id) {
				double old_entropy = association.getMatrix()[p_b_id][p_a_id].getValue();					
				double new_entropy = entropy * (1 - Global.expAvgWt) 
						+ old_entropy * Global.expAvgWt;						

				association.getMatrix()[p_b_id][p_a_id].setValue(new_entropy);
				
			} else {
				double old_entropy = association.getMatrix()[p_a_id][p_b_id].getValue();
				double new_entropy = entropy * (1 - Global.expAvgWt) 
						+ old_entropy * Global.expAvgWt;
					
				association.getMatrix()[p_a_id][p_b_id].setValue(new_entropy);
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
		entropy = p_total * (-(p_a_trData/p_total) * (Math.log(p_a_trData/p_total)/Math.log(2))
						 -(p_b_trData/p_total) * (Math.log(p_b_trData/p_total)/Math.log(2)));
		
		return entropy;
	}
	
// Migration decision	
	public static IntPair migrationDecision(Cluster cluster) {
		
		ArrayList<Element> swappingCandidates = new ArrayList<Element>();			
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
				
				for(int p_b_id : s_b.getServer_partitions()) {
					
					Partition p_b = cluster.getPartition(p_b_id);
					MatrixElement me = null;
					
					if(p_a_id > p_b_id)
						me = association.getMatrix()[p_a_id][p_b_id];
					else
						me = association.getMatrix()[p_b_id][p_a_id];
						
					Element e = new Element(me.getId());
					
					e.p_pair.x = me.getRow_pos();
					e.p_pair.y = me.getCol_pos();
					
					e.s_pair.x = s_a.getServer_id();
					e.s_pair.y = s_b.getServer_id();
					
					e.p_dataPair.x = p_a.getPartition_dataSet().size();
					e.p_dataPair.y = p_b.getPartition_dataSet().size();
					
					e.swapping_gain = getSwappingGain(cluster, e, p_a, p_b);
					e.p_data_distance = getDataDistance(e);
					
					swappingCandidates.add(e);
					
				} // end-for()
			} // end-for()
		} // end-while()
		
//		// Testing
//		System.out.println("--> List of potential swapping candidates: ");
//		for(Element e : swappingCandidates) {
//			System.out.println("\t\t"+e.toString());
//		}
		
		// Sort the array list by swapping gain
		Collections.sort(swappingCandidates, new Comparator<Element>(){
			@Override
			public int compare(Element e1, Element e2) {					
				return (((double)e1.swapping_gain > (double)e2.swapping_gain) ? -1 : 
        			((double)e1.swapping_gain < (double)e2.swapping_gain) ? 1 : 0);
			}
		});
		
		// Rank based on swapping gain (descending order)
		int rank = swappingCandidates.size();
		for(int i = 0; i < swappingCandidates.size(); i++) {
			if(i != 0)
				if(swappingCandidates.get(i).swapping_gain != swappingCandidates.get(i-1).swapping_gain)
					--rank;
			
			swappingCandidates.get(i).gain_rank = rank * Global.idt_priority; // Prioritise gain (alpha)
		}
		
//		// Testing
//		System.out.println("--> List of potential swapping candidates (sorted based on swapping gain): ");
//		for(Element e : swappingCandidates) {
//			System.out.println("\t\t"+e.toString());
//		}

		// Sort the array list by distance
		Collections.sort(swappingCandidates, new Comparator<Element>(){
			@Override
			public int compare(Element e1, Element e2) {					
				return (((int)e1.p_data_distance < (int)e2.p_data_distance) ? -1 : 
        			((int)e1.p_data_distance > (int)e2.p_data_distance) ? 1 : 0);
			}
		});

		// Rank based on distance (ascending order)
		rank = swappingCandidates.size();
		for(int i = 0; i < swappingCandidates.size(); i++) {
			if(i != 0)
				if(swappingCandidates.get(i).p_data_distance != swappingCandidates.get(i-1).p_data_distance) 
					--rank;
			
			swappingCandidates.get(i).distance_rank = rank*(1 - Global.idt_priority); // Prioritise balance (1-alpha)	
		}

		// Testing
//		System.out.println("--> List of potential swapping candidates (sorted based on distance): ");
//		for(Element e : swappingCandidates) {
//			System.out.println("\t\t"+e.toString());
//		}
		
		// Assign combined rank		
		for(Element e : swappingCandidates)
			e.combined_rank = (e.gain_rank + e.distance_rank);		
		
		// Sort the array list by the value of combined rank (descending order)
		Collections.sort(swappingCandidates, new Comparator<Element>(){
			@Override
			public int compare(Element e1, Element e2) {					
				return (((double)e1.combined_rank > (double)e2.combined_rank) ? -1 : 
        			((double)e1.combined_rank < (double)e2.combined_rank) ? 1 : 0);
			}
		});
		
		// Select the element with maximum combined rank value
		if(swappingCandidates.size() != 0) {			
			
			Global.LOGGER.info("----------------------------------------------");
			Global.LOGGER.info("Sorted list of potential swapping pairs:");
			
			for(Element p : swappingCandidates)
				Global.LOGGER.info("--"+p.toString());
						
			Element selected = swappingCandidates.get(0);
			
			Global.LOGGER.info("----------------------------------------------");
			Global.LOGGER.info("Selected swapping pair: "+selected.toString());
					
			return (new IntPair(selected.p_pair.x, selected.p_pair.y));
		
		} else {
			return null;
		}
	}
	
	// Returns the swapping gain	
	private static double getSwappingGain(Cluster cluster, Element e, Partition p_x, Partition p_y) {		
		
		double p_x_gain = getGain(cluster, p_x, p_y);
		double p_y_gain = getGain(cluster, p_y, p_x);
		
		return (p_x_gain + p_y_gain);
	}
	
	// Returns the actual gain for a migrating partition P_x into server S_y 
	// x = source, y = target
	private static double getGain(Cluster cluster, Partition p_i, Partition p_j) {
		
		int p_i_id = p_i.getPartition_id();
		
		Server s_i = cluster.getServer(p_i.getPartition_serverId());
		Server s_j = cluster.getServer(p_j.getPartition_serverId());
		
		// Get the gain for Pi
		double gain = 0.0d;
		double value = 0.0d;
		
		for(int p_id : s_j.getServer_partitions()) {
			
			if(p_i_id > p_id)			
				value = association.getMatrix()[p_i_id][p_id].getValue();
			else
				value = association.getMatrix()[p_id][p_i_id].getValue();
			
			gain += value;
		}
		
		// Get the loss for Pj
		double loss = 0.0d;
		for(int p_id : s_i.getServer_partitions()) {
			
			if(p_i_id > p_id)		
				value = association.getMatrix()[p_i_id][p_id].getValue();
			else
				value = association.getMatrix()[p_id][p_i_id].getValue();
			
			if(p_i_id != p_id)
				loss += value;
		}
				
		// Returns the actual gain
		return (gain - loss);
	}
	
	// Returns the difference of data volume of two partitions as a distance
	private static int getDataDistance(Element e) {		
		return Math.abs(e.p_dataPair.x - e.p_dataPair.y);
	}	
}

// A specialised Element Class
class Element {
	int id;
	IntPair p_pair;
	IntPair s_pair;
	IntPair p_dataPair;
	double swapping_gain;
	int p_data_distance;
	double gain_rank;
	double distance_rank;
	double combined_rank;
	
	public Element(int id) {
		this.setId(id);
		this.p_pair = new IntPair(-1, -1);
		this.s_pair = new IntPair(-1, -1);
		this.p_dataPair = new IntPair(-1, -1);
		this.swapping_gain = Double.MIN_VALUE;
		this.p_data_distance = Integer.MIN_VALUE;
		this.gain_rank = -1;
		this.distance_rank = -1;
		this.combined_rank = -1;
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
		return ("P"+this.p_pair+" | S"+this.s_pair+" | Pdata"+this.p_dataPair
				+" [Swapping Gain = "+this.swapping_gain+"|"+" Data Distance = "+this.p_data_distance+"]");	
	}
}