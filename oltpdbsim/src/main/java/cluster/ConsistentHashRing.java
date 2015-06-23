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

package main.java.cluster;

import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRing<T> {
	
	private final SortedMap<Long, T> ring;
	private final int replicas;

	public ConsistentHashRing(int replicas) {
		this.replicas = replicas;
		this.ring = new TreeMap<Long, T>();
	}

	// Add
	public void add(T node) {
		for (int i = 1; i <= this.replicas; i++) {
			this.ring.put(Long.parseLong(node.toString()), node);    	
			//this.ring.put(Utility.sha1Hash(node.toString() + i), node);
			//this.ring.put(Utility.intHash(Integer.parseInt(node.toString())), node);
		}
  }

	// Remove
	public void remove(T node) {
		for (int i = 1; i <= this.replicas; i++)
			this.ring.remove(node.toString());    
			//this.ring.remove(Utility.sha1Hash(node.toString() + i));
			//this.ring.remove(Utility.intHash(Integer.parseInt(node.toString())));
	}

	// Get
	public T get(long hash) {
	  
		if (this.ring.isEmpty())
			return null;
    
		long key = -1;
		if (!this.ring.containsKey(hash)) {    	
			SortedMap<Long, T> tailMap = this.ring.tailMap(hash);  
			key = tailMap.isEmpty() ? this.ring.firstKey() : tailMap.firstKey();
			//System.out.println(">> key = "+key+" | hash = "+hash+" | "+tailMap+" | "+this.ring.firstKey());
		}
    
		return this.ring.get(key);
	}
}