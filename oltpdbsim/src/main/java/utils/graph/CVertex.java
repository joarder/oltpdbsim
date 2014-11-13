package main.java.utils.graph;

import java.util.Map;
import java.util.HashMap;
import java.util.Map.Entry;

public class CVertex extends SimpleVertex {

	private Map<Integer, SimpleVertex> VSet;
	
	public CVertex(int id, int weight) {
		super(id, weight);		
		this.VSet = new HashMap<Integer, SimpleVertex>();
	}
	
	public Map<Integer, SimpleVertex> getVSet() {
		return VSet;
	}

	public void setVSet(Map<Integer, SimpleVertex> VSet) {
		this.VSet = VSet;
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
		if (!(object instanceof CVertex)) {
			return false;
		}
		
		CVertex cv = (CVertex) object;
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
	public int compareTo(CVertex cv) {		
		return (((int)this.getId() < (int)cv.getId()) ? -1 : 
			((int)this.getId() > (int)cv.getId()) ? 0 : 1);		
	}
}