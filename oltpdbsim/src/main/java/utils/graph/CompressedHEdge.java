package main.java.utils.graph;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

public class CompressedHEdge extends SimpleHEdge {

	private Map<Integer, SimpleHEdge> HSet;	
	private double c_e; // Contribution of each hyperedge e in H_cut towards total number of distributed transactions seen so far
	private int ndt_e;
	
	public CompressedHEdge(int id, int weight) {
		super(id, weight);		
		HSet = new HashMap<Integer, SimpleHEdge>();
		c_e = 0.0;
		ndt_e = 0;		
	}	

	public Map<Integer, SimpleHEdge> getHESet() {
		return HSet;
	}

	public void setHESet(Map<Integer, SimpleHEdge> HSet) {
		this.HSet = HSet;
	}

	public double getC_e() {
		return c_e;
	}

	public void setC_e(double c_e) {
		this.c_e = c_e;
	}

	public int getNdt_e() {
		return ndt_e;
	}

	public void setNdt_e(int ndt_e) {
		this.ndt_e = ndt_e;
	}

	public void updateWeight() {
		int weight = 0;
		
		for(Entry<Integer, SimpleHEdge> h : HSet.entrySet()) {
			weight += h.getValue().getWeight();
		}
		
		this.setWeight(weight);
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof CompressedHEdge)) {
			return false;
		}
		
		CompressedHEdge ce = (CompressedHEdge) object;
		return (this.getId() == ce.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getId();
		return result;
	}
	
	public int compareTo(CompressedHEdge ce) {		
		return (((int)this.getId() < (int)ce.getId()) ? -1 : 
			((int)this.getId() > (int)ce.getId()) ? 1 : 0);		
	}
	
	@Override
	public String toString() {
		return ("CE"+this.getId()+"|"+this.getHESet());
	}
	
//	@Override
//	public String toString() {
//		return ("CE ["+this.getId()+" | "+c_e+" | "+ndt_e+" | "+nh_i+"]");
//	}
}