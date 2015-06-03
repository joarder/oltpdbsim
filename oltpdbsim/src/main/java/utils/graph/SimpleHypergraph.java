package main.java.utils.graph;

import java.util.Map;
import java.util.Set;

import main.java.cluster.Cluster;
import main.java.workload.WorkloadBatch;
import edu.uci.ics.jung.graph.Hypergraph;

public interface SimpleHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
	extends Hypergraph<V, H> {
		
	boolean addHEdge(H h, Set<V> vSet);
	boolean removeHEdge(H h);
	H getHEdge(int id);
	H getHEdge(Set<V> vSet);
	V getVertex(int id);
	Set<V> getVertices(H h);
	boolean addCHEdge(H h);
	boolean removeCHEdge(H h);
	boolean addCVertex(V v);
	boolean removeCVertex(V v);
	Map<CompressedHEdge, Set<CompressedVertex>> getcHEdges();
	Map<CompressedVertex, Set<CompressedHEdge>> getcVertices();
	CompressedHEdge getCHEdge(H h);
	CompressedHEdge getCHEdge(Set<CompressedVertex> vSet);
	CompressedHEdge getCHEdge(int id);
	boolean isSpans2Server(Cluster cluster, WorkloadBatch wb, CompressedHEdge ch);
	Set<CompressedVertex> getIncidentCVertices(CompressedHEdge ch);
	Set<Integer> getIncidentPartitions(WorkloadBatch wb, CompressedHEdge ch);
	Set<Integer> getIncidentServers(WorkloadBatch wb, CompressedHEdge ch);
}