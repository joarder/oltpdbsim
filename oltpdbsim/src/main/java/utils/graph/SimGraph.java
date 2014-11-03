package main.java.utils.graph;

import java.io.Serializable;

import main.java.entry.Global;
import edu.uci.ics.jung.graph.UndirectedSparseGraph;

@SuppressWarnings("serial")
public class SimGraph<V extends SimpleVertex, E extends SimpleEdge> 
	extends UndirectedSparseGraph<V, E> implements SimpleGraph<V, E>, Serializable {

	
	// Adds a new edge in the graph
	public void addGraphEdge(E e, V v1, V v2) {        
        
        this.addEdge(e, v1, v2);
        
        if(Global.compressionEnabled)
        	this.addCompressedGraphEdge(e);
    }
	
	private void addCompressedGraphEdge(E e) {	
		
	}

	public E getEdge(int id) {

		for(E e : this.edges.keySet()) {
			if(e.getId() == id)
				return e;
		}
		
		return null;
	}
	
	public V getVertex(int id) {
		
		for(V v : this.vertices.keySet()) {
			if(v.getId() == id)
				return v;
		}
		
		return null;
	}
}