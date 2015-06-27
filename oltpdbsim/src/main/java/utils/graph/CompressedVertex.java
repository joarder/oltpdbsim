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
import java.util.Map.Entry;
import java.util.TreeMap;

public class CompressedVertex extends SimpleVertex {
	
	private Map<Integer, SimpleVertex> VSet;
	
	public CompressedVertex(int id, int weight, int pid, int sid) {
		super(id, id, weight, pid, sid);
		this.setVSet(new TreeMap<Integer, SimpleVertex>());
	}
	
	public Map<Integer, SimpleVertex> getVSet() {
		return VSet;
	}

	public void setVSet(Map<Integer, SimpleVertex> VSet) {
		this.VSet = VSet;
	}

	public void updateWeight() {
		int weight = 0;
		
		for(Entry<Integer, SimpleVertex> v : VSet.entrySet()) {
			weight += v.getValue().getWeight();
		}
		
		this.setWeight(weight);
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof CompressedVertex)) {
			return false;
		}
		
		CompressedVertex cv = (CompressedVertex) object;
		return (this.getId() == cv.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getId();
		return result;
	}
	
	// Can not override super class compareTo here	
	public int compareTo(CompressedVertex cv) {		
		return (((int)this.getId() < (int)cv.getId()) ? -1 : 
			((int)this.getId() > (int)cv.getId()) ? 0 : 1);		
	}
	
	
	@Override
	public String toString() {
		return ("CV"+this.getId()+"("+this.getWeight()+") | "+this.getVSet());
	}
}