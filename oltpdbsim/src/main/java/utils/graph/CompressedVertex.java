package main.java.utils.graph;

import java.util.Map;
import java.util.TreeMap;

public class CompressedVertex extends SimpleVertex {

	private Map<Integer, SimpleVertex> VSet;
	
	public CompressedVertex(int id, int weight) {
		super(id, weight);		
		this.VSet = new TreeMap<Integer, SimpleVertex>();
	}
	
	public Map<Integer, SimpleVertex> getVSet() {
		return VSet;
	}

	public void setVSet(Map<Integer, SimpleVertex> VSet) {
		this.VSet = VSet;
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
		
		CompressedVertex c = (CompressedVertex) object;
		return (this.getId() == c.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getId();
		return result;
	}
	
	public int compareTo(CompressedVertex c) {		
		return (((int)this.getId() < (int)c.getId()) ? -1 : 
			((int)this.getId() > (int)c.getId()) ? 0 : 1);		
	}
}