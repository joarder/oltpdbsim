/*******************************************************************************
 * Copyright [2014] [Joarder Kamal]
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/

package main.java.utils.graph;

import java.util.Map;
import java.util.Set;

import main.java.workload.WorkloadBatch;
import edu.uci.ics.jung.graph.Hypergraph;

public interface ISimpleHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
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
	Set<CompressedVertex> getIncidentCVertices(CompressedHEdge ch);
	Set<Integer> getIncidentPartitions(WorkloadBatch wb, CompressedHEdge ch);
	Set<Integer> getIncidentServers(WorkloadBatch wb, CompressedHEdge ch);
	void updateHEdgeWeight(H h, int weight);
	void updateVertexWeight(V v);
}