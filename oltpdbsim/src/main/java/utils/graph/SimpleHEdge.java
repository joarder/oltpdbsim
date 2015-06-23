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

public class SimpleHEdge implements Comparable<SimpleHEdge> {

	private int id;
	private int weight;
	
	public SimpleHEdge(int id, int weight) {
		this.setId(id);
		this.setWeight(weight);
	}
	
	public int getId() {
		return id;
	}
	
	public void setId(int id) {
		this.id = id;
	}
	
	public int getWeight() {
		return weight;
	}
	
	public void setWeight(int tr_frequency) {
		this.weight = tr_frequency;
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof SimpleHEdge))
			return false;
		
		SimpleHEdge e = (SimpleHEdge) object;
		return (this.getId() == e.getId());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.id;
		return result;
	}

	@Override
	public int compareTo(SimpleHEdge e) {		
		return (((int)this.getId() < (int)e.getId()) ? -1 : 
			((int)this.getId() > (int)e.getId()) ? 1 : 0);		
	}
	
	@Override
	public String toString() {		
		return ("HE"+this.getId()+"("+this.weight+")");
	}
}