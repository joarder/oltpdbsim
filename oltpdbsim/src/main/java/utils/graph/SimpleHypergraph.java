package main.java.utils.graph;

import java.util.Map;
import java.util.Set;

import edu.uci.ics.jung.graph.Hypergraph;

public interface SimpleHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
	extends Hypergraph<V, H> {
		
	Map<CompressedHEdge, Set<CompressedVertex>> getcHEdges();
	Map<CompressedVertex, Set<CompressedHEdge>> getcVertices();
	void addHEdge(H h, Set<V> vSet);
	CompressedHEdge addCompressedHEdge(H h);
	CompressedVertex addCompressedVertex(V v);
	H getHEdge(int id);
	H getHEdge(Set<V> vSet);
	V getVertex(int id);
}