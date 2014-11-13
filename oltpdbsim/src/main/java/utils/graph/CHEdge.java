package main.java.utils.graph;

import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.HashSet;
import java.util.Map.Entry;

public class CHEdge extends SimpleHEdge {

	private Map<Integer, SimpleHEdge> HSet;
	
	SortedMap<Integer, SortedMap<Integer, Integer>> nh_i;	
	Set<Integer> v_i; // virtual data
	Set<Integer> s_i; // physical server
	double c_e; // C_e
	int ndt_e;	// ndt_e
	
	public CHEdge(int id, int weight) {
		super(id, weight);		
		HSet = new HashMap<Integer, SimpleHEdge>();
		
		nh_i = new TreeMap<Integer, SortedMap<Integer, Integer>>();
		v_i = new HashSet<Integer>();
		s_i = new HashSet<Integer>();
		c_e = 0.0;
		ndt_e = 0;		
	}	

	public Map<Integer, SimpleHEdge> getHESet() {
		return HSet;
	}

	public void setHESet(Map<Integer, SimpleHEdge> HSet) {
		this.HSet = HSet;
	}

	public SortedMap<Integer, SortedMap<Integer, Integer>> getNh_i() {
		return nh_i;
	}

	public void setNh_i(SortedMap<Integer, SortedMap<Integer, Integer>> nh_i) {
		this.nh_i = nh_i;
	}

	public Set<Integer> getV_i() {
		return v_i;
	}

	public void setV_i(Set<Integer> v_i) {
		this.v_i = v_i;
	}

	public Set<Integer> getS_i() {
		return s_i;
	}

	public void setS_i(Set<Integer> s_i) {
		this.s_i = s_i;
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

	public int getTotalNHi(int s_id) {
		int sum = 0;
		
		for(Entry<Integer, SortedMap<Integer, Integer>> entry : nh_i.entrySet()) {
			for(Entry<Integer, Integer> e : entry.getValue().entrySet()) {
				if(e.getKey() == s_id)
					sum += e.getValue();
			}
		}
		
		return sum;
	}
		
//	@Override
//	public void incWeight(int weight) {
//		int w = this.getWeight();
//		this.setWeight(w + weight);
//	}
//	
//	@Override
//	public void decWeight(int weight) {
//		int w = this.getWeight();
//		this.setWeight(w - weight);
//	}

	public void updateWeight() {
		int weight = 0;
		
		for(Entry<Integer, SimpleHEdge> h : HSet.entrySet()) {
			weight += h.getValue().getWeight();
		}
		
		this.setWeight(weight);
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof CHEdge)) {
			return false;
		}
		
		CHEdge ce = (CHEdge) object;
		return (this.getId() == ce.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getId();
		return result;
	}
	
	public int compareTo(CHEdge ce) {		
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