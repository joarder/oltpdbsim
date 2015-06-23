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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class CompressedHEdge extends SimpleHEdge {

	private Map<Integer, SimpleHEdge> HSet;
	
	public CompressedHEdge(int id, int d) {
		super(id, d);		
		HSet = new HashMap<Integer, SimpleHEdge>();		
	}	

	public Map<Integer, SimpleHEdge> getHESet() {
		return HSet;
	}

	public void setHESet(Map<Integer, SimpleHEdge> HSet) {
		this.HSet = HSet;
	}		
	
	public void updateWeight() {
		int weight = 0;
		
		for(Entry<Integer, SimpleHEdge> h : HSet.entrySet()) {
			weight += h.getValue().getWeight();
		}
		
		this.setWeight(weight);
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof CompressedHEdge)) {
			return false;
		}
		
		CompressedHEdge ce = (CompressedHEdge) object;
		return (this.getId() == ce.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getId();
		return result;
	}
	
	public int compareTo(CompressedHEdge ce) {		
		return (((int)this.getId() < (int)ce.getId()) ? -1 : 
			((int)this.getId() > (int)ce.getId()) ? 1 : 0);		
	}
	
	@Override
	public String toString() {
		return ("CHE"+this.getId()+"("+this.getWeight()+") | "+this.getHESet());
	}	
}