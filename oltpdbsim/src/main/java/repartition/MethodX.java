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

public class MethodX {

	public Matrix association;

	public void init(Cluster cluster) {		
		int M = cluster.getPartitions().size()+1;
		int N = M;				
		association = Utility.createMatrix(M, N);
	}
	
	public void updateAssociation(Cluster cluster, Transaction tr) {
		// Based on: https://code.google.com/p/combinatoricslib/
		// Get all the pairs of combinations of Partition accessed by this Transaction
		// Create the initial vector
		ICombinatoricsVector<Integer> initialVector = 
				Factory.createVector(tr.getTr_partitionSet().keySet());

		// Create a simple combination generator to generate 2-combinations of the initial vector
		Generator<Integer> gen = Factory.createSimpleCombinationGenerator(initialVector, 2);
		
		// Print all possible combinations
		for (ICombinatoricsVector<Integer> combination : gen) {			
		
			int Pa = combination.getValue(0);
			int Pb = combination.getValue(1);
			
			int Pa_data = tr.getTr_partitionSet().get(Pa).size();
			int Pb_data = tr.getTr_partitionSet().get(Pb).size();
			
			double entropy = this.getEntropy(Pa_data, Pb_data);
			
			if(Pb > Pa) {
				double val = association.getMatrix()[Pb][Pa].getValue();
				association.getMatrix()[Pb][Pa].setValue(val+entropy);
			} else {
				double val = association.getMatrix()[Pa][Pb].getValue();
				association.getMatrix()[Pa][Pb].setValue(val+entropy);
			}
		}
		
		// Verification
//		System.out.println("====================================================================");
//		association.print();
		//this.migrationDecision(cluster);
	}
	
	private double getEntropy(int Pa_data, int Pb_data) {
		double entropy = 0.0d;
		
		double total = Pa_data + Pb_data;
		entropy = total * (-(Pa_data/total) * (Math.log(Pa_data/total)/Math.log(2))
						 -(Pb_data/total) * (Math.log(Pb_data/total)/Math.log(2)));
		
		return entropy;
	}
	
// Migration decision	
	public IntPair migrationDecision(Cluster cluster) {
		
		ArrayList<Element> swappingCandidates = new ArrayList<Element>();			
		Set<Integer> serverSet = new TreeSet<Integer>();
		
		for(Server s : cluster.getServers())
			serverSet.add(s.getServer_id());
		
		ICombinatoricsVector<Integer> initialVector = Factory.createVector(serverSet);

		// Create a simple combination generator to generate 2-combinations of the initial vector
		Generator<Integer> gen = Factory.createSimpleCombinationGenerator(initialVector, 2);
		
		Server S_a = null;
		Server S_b = null;
		
		// Print all possible combinations
		for (ICombinatoricsVector<Integer> combination : gen) {	
			
			S_a = cluster.getServer(combination.getValue(0));
			S_b = cluster.getServer(combination.getValue(1));						
			
			for(int Pa : S_a.getServer_partitions()) {
				Partition P_a = cluster.getPartition(Pa);
				
				for(int Pb : S_b.getServer_partitions()) {
					
					Partition P_b = cluster.getPartition(Pb);
					MatrixElement me = null;
					
					if(Pa > Pb)
						me = association.getMatrix()[Pa][Pb];
					else
						me = association.getMatrix()[Pb][Pa];
						
					Element e = new Element(me.getId());
					
					e.PPair.x = me.getRow_pos();
					e.PPair.y = me.getCol_pos();
					
					e.SPair.x = S_a.getServer_id();
					e.SPair.y = S_b.getServer_id();
					
					e.PdataPair.x = P_a.getPartition_dataSet().size();
					e.PdataPair.y = P_b.getPartition_dataSet().size();
					
					e.swapping_gain = getSwappingGain(cluster, e, P_a, P_b);
					e.data_distance = getDataDistance(e);
					
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
			
			swappingCandidates.get(i).gain_rank = rank * Global.priority; // Prioritise gain (alpha)
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
				return (((int)e1.data_distance < (int)e2.data_distance) ? -1 : 
        			((int)e1.data_distance > (int)e2.data_distance) ? 1 : 0);
			}
		});

		// Rank based on distance (ascending order)
		rank = swappingCandidates.size();
		for(int i = 0; i < swappingCandidates.size(); i++) {
			if(i != 0)
				if(swappingCandidates.get(i).data_distance != swappingCandidates.get(i-1).data_distance) 
					--rank;
			
			swappingCandidates.get(i).distance_rank = rank*(1 - Global.priority); // Prioritise balance (1-alpha)	
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
			
//			Global.LOGGER.info("----------------------------------------------");
//			Global.LOGGER.info("Sorted list of potential swapping pairs:");
			
//			for(Element p : swappingCandidates)
//				Global.LOGGER.info("--"+p.toString());
						
			Element selected = swappingCandidates.get(0);
			
			Global.LOGGER.info("----------------------------------------------");
			Global.LOGGER.info("Selected swapping pair: "+selected.toString());
					
			return (new IntPair(selected.PPair.x, selected.PPair.y));
		
		} else {
			return null;
		}
	}
	
	// Returns the swapping gain	
	private double getSwappingGain(Cluster cluster, Element e, Partition P_a, Partition P_b) {		
		
		double Pa_gain = getGain(cluster, P_a, P_b);
		double Pb_gain = getGain(cluster, P_b, P_a);
		
		return (Pa_gain + Pb_gain);
	}
	
	// Returns the actual gain for a migrating partition P_x into server S_y 
	// x = source, y = target
	private double getGain(Cluster cluster, Partition P_x, Partition P_y) {
		
		int Px = P_x.getPartition_id();
		
		Server S_x = cluster.getServer(P_x.getPartition_serverId());
		Server S_y = cluster.getServer(P_y.getPartition_serverId());
		
		// Get the gain
		double gain = 0.0d;
		double value = 0.0d;
		
		for(int Py_id : S_y.getServer_partitions()) {
			
			if(Px > Py_id)			
				value = association.getMatrix()[Px][Py_id].getValue();
			else
				value = association.getMatrix()[Py_id][Px].getValue();
			
			gain += value;
		}
		
		// Get the loss
		double loss = 0.0d;
		for(int Px_id : S_x.getServer_partitions()) {
			
			if(Px > Px_id)		
				value = association.getMatrix()[Px][Px_id].getValue();
			else
				value = association.getMatrix()[Px_id][Px].getValue();
			
			if(Px != Px_id)
				loss += value;
		}
				
		// Returns the actual gain
		return (gain - loss);
	}
	
	// Returns the difference of data volume of two partitions as a distance
	private int getDataDistance(Element e) {		
		return Math.abs(e.PdataPair.x - e.PdataPair.y);
	}	
}

// A specialised Element Class
class Element {
	int id;
	IntPair PPair;
	IntPair SPair;
	IntPair PdataPair;
	double swapping_gain;
	int data_distance;
	double gain_rank;
	double distance_rank;
	double combined_rank;
	
	public Element(int id) {
		this.setId(id);
		this.PPair = new IntPair(-1, -1);
		this.SPair = new IntPair(-1, -1);
		this.PdataPair = new IntPair(-1, -1);
		this.swapping_gain = Double.MIN_VALUE;
		this.data_distance = Integer.MIN_VALUE;
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
		return ("P"+this.PPair+" | S"+this.SPair+" | Pdata"+this.PdataPair
				+" [Swapping Gain = "+this.swapping_gain+"|"+" Data Distance = "+this.data_distance+"]");	
	}
}