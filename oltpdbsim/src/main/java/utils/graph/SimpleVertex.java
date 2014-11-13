package main.java.utils.graph;

public class SimpleVertex implements Comparable<SimpleVertex> {
	
	private int id;
	private int weight;
	
	public SimpleVertex(int id, int weight) {
		this.setId(id);
		this.setWeight(weight);
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}
	
//	public void incWeight(int weight) {
//		int w = this.getWeight();
//		this.setWeight(w + weight);
//	}
//	
//	public void decWeight(int weight) {
//		int w = this.getWeight();
//		this.setWeight(w - weight);
//	}
	
	@Override
	public String toString() {		
		return ("V"+this.getId());
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof SimpleVertex)) {
			return false;
		}
		
		SimpleVertex v = (SimpleVertex) object;
		return (this.getId() == v.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.id;
		return result;
	}
	
	@Override
	public int compareTo(SimpleVertex v) {		
		return (((int)this.getId() < (int)v.getId()) ? -1 : 
			((int)this.getId() > (int)v.getId()) ? 1 : 0);		
	}
}