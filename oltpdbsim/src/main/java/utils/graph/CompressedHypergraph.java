package main.java.utils.graph;

import java.util.Set;
import edu.uci.ics.jung.graph.Hypergraph;

public interface CompressedHypergraph<V extends CompressedVertex, H extends CompressedHEdge> 
	extends Hypergraph<V, H> {

	boolean addCHEdge(H h, Set<V> vSet);
	H getCHEdge(int id);
	H getCHEdge(Set<V> vSet);
	V getCVertex(int id);
}