package main.java.utils.graph;

import java.util.Map;
import java.util.Set;

import edu.uci.ics.jung.graph.Hypergraph;

public interface SimpleHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
	extends Hypergraph<V, H> {
		
	boolean addHEdge(H h, Set<V> vSet);
	boolean removeHEdge(H h);
	H getHEdge(int id);
	H getHEdge(Set<V> vSet);
	V getVertex(int id);
	boolean addCHEdge(H h);
	boolean removeCHEdge(H h);
	boolean addCVertex(V v);
	boolean removeCVertex(V v);
	Map<CHEdge, Set<CVertex>> getcHEdges();	
}