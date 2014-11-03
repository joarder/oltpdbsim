package main.java.utils.graph;

public class SimpleEdge extends SimpleHEdge {

	public SimpleEdge(int id, int weight) {
		super(id, weight);
	}
	
	@Override
	public String toString() {		
		return ("E"+this.getId());
	}
}