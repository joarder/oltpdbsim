package main.java.utils.graph;

public class SimpleVertex implements Comparable<SimpleVertex> {
	
	private int id;
	private int weight; // Sum of weights of the incident hyperedges
	private int partition_id; // Residing Partition id
	private int server_id; // Residing Partition id
	
	public SimpleVertex(int id, int weight, int partition_id, int server_id) {
		this.setId(id);
		this.setWeight(weight);
		this.setPartition_id(partition_id);
		this.setServer_id(server_id);
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
	
	public int getPartition_id() {
		return partition_id;
	}

	public void setPartition_id(int pid) {
		this.partition_id = pid;
	}

	public int getServer_id() {
		return server_id;
	}

	public void setServer_id(int sid) {
		this.server_id = sid;
	}

	@Override
	public String toString() {		
		return ("V"+this.getId()+"("+this.getWeight()+")");
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