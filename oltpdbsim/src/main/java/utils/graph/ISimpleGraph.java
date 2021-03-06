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

import edu.uci.ics.jung.graph.UndirectedGraph;

public interface ISimpleGraph<V extends SimpleVertex, E extends SimpleEdge> 
	extends UndirectedGraph<V, E> {

	void addGraphEdge(E e, V v1, V v2);
	E getEdge(int id);
	V getVertex(int id);
}