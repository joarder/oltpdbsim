package main.java.utils.graph;

public class SimpleHEdge implements Comparable<SimpleHEdge> {

	private int id;
	private int weight;
	
	public SimpleHEdge(int id, int weight) {
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