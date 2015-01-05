package main.java.utils.graph;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

public class CompressedVertex extends SimpleVertex {
	
	private Map<Integer, SimpleVertex> VSet;
	private int nh;
	
	public CompressedVertex(int id, int weight, int pid, int sid) {
		super(id, weight, pid, sid);
		this.setVSet(new HashMap<Integer, SimpleVertex>());
		this.setNh(0);
	}
	
	public Map<Integer, SimpleVertex> getVSet() {
		return VSet;
	}

	public void setVSet(Map<Integer, SimpleVertex> VSet) {
		this.VSet = VSet;
	}
	
	public int getNh() {
		return nh;
	}

	public void setNh(int nh) {
		this.nh = nh;
	}

	public void updateWeight() {
		int weight = 0;
		
		for(Entry<Integer, SimpleVertex> v : VSet.entrySet()) {
			weight += v.getValue().getWeight();
		}
		
		this.setWeight(weight);
	}
	
	@Override
	public String toString() {		
		return ("CV"+this.getId()+"|"+this.getVSet());
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof CompressedVertex)) {
			return false;
		}
		
		CompressedVertex cv = (CompressedVertex) object;
		return (this.getId() == cv.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getId();
		return result;
	}
	
	// Can not override super class compareTo here	
	public int compareTo(CompressedVertex cv) {		
		return (((int)this.getId() < (int)cv.getId()) ? -1 : 
			((int)this.getId() > (int)cv.getId()) ? 0 : 1);		
	}
}