package main.java.utils.graph;

import edu.uci.ics.jung.graph.UndirectedGraph;

public interface SimpleGraph<V extends SimpleVertex, E extends SimpleEdge> 
	extends UndirectedGraph<V, E> {

	void addGraphEdge(E e, V v1, V v2);
	E getEdge(int id);
	V getVertex(int id);
}