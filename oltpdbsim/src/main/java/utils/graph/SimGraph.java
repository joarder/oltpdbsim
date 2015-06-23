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