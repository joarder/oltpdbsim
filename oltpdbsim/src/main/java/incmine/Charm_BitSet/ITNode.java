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
/**
 * Source: http://www.cs.waikato.ac.nz/~abifet/MOA-IncMine/
 */

package main.java.incmine.Charm_BitSet;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Set;

public class ITNode {
	Itemset itemsetObject = new Itemset();
	private ITNode parent = null;
	private List<ITNode> childNodes = new ArrayList<ITNode>();
	
	public int size(){
		return itemsetObject.cardinality;
	}
	

	public double getRelativeSupport(int nbObject) {
		return ((double) itemsetObject.cardinality) / ((double) nbObject);
	}
	
	public ITNode(Set<Integer> itemset){
		this.itemsetObject.itemset = itemset;
	}

	public Set<Integer> getItemset() {
		return itemsetObject.itemset;
	}

	public BitSet getTidset() {
		return itemsetObject.tidset;
	}

	public void setTidset(BitSet tidset, int cardinality) {
		this.itemsetObject.tidset = tidset;
		this.itemsetObject.cardinality = cardinality;
	}

	public List<ITNode> getChildNodes() {
		return childNodes;
	}

	public ITNode getParent() {
		return parent;
	}

	public void setParent(ITNode parent) {
		this.parent = parent;
	}
	
	// for charm
	public void replaceInChildren(Set<Integer> replacement) {
		for(ITNode node : getChildNodes()){
			Set<Integer> itemset  = node.getItemset();
			for(Integer item : replacement){
				if(!itemset.contains(item)){
					itemset.add(item);
				}
			}
			node.replaceInChildren(replacement);
		}
	}


	public void setItemset(Set<Integer> union) {
		this.itemsetObject.itemset = union;
	}


}
