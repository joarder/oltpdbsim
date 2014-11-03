package main.java.utils.graph;

import edu.uci.ics.jung.graph.Graph;

public interface SimpleGraph<V extends SimpleVertex, E extends SimpleEdge> 
	extends Graph<V, E> {

	void addGraphEdge(E e, V v1, V v2);
	E getEdge(int id);
	V getVertex(int id);
}