package main.java.utils.graph;

public class SimpleHEdge implements Comparable<SimpleHEdge> {

	private int id;
	private double weight;
	
	public SimpleHEdge(int id, double weight) {
		this.setId(id);
		this.setWeight(weight);
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public double getWeight() {
		return weight;
	}
	
	public void setWeight(double tr_frequency) {
		this.weight = tr_frequency;
	}
	
	@Override
	public String toString() {		
		return ("HE"+this.getId());
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof SimpleHEdge))
			return false;
		
		SimpleHEdge e = (SimpleHEdge) object;
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
	public int compareTo(SimpleHEdge e) {		
		return (((int)this.getId() < (int)e.getId()) ? -1 : 
			((int)this.getId() > (int)e.getId()) ? 1 : 0);		
	}
}