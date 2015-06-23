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
import java.util.List;

public class HashTable {
	
	int size;
	List<Itemset>[] table;
	
	@SuppressWarnings("unchecked")
	public HashTable(int size){
		this.size = size;
		table = new ArrayList[size];
	}
	public boolean containsSupersetOf(Itemset itemsetObject) {
		int hashcode = hashCode(itemsetObject);
		if(table[hashcode] ==  null){
			return false;
		}
		for(Object object : table[hashcode]){
			Itemset itemsetObject2 = (Itemset)object;
			if(itemsetObject2.itemset.size() == itemsetObject.itemset.size() &&
					itemsetObject2.itemset.containsAll(itemsetObject.itemset)
					){  // FIXED BUG 2010-10: containsAll instead of contains.
				return true;
			}
		}
		return false;
	}
	public void put(Itemset itemsetObject) {
		int hashcode = hashCode(itemsetObject);
		if(table[hashcode] ==  null){
			table[hashcode] = new ArrayList<Itemset>();
		}
		table[hashcode].add(itemsetObject);
	}
	
	public int hashCode(Itemset itemsetObject){
		int hashcode =0;
//		for (int bit = bitset.nextSetBit(0); bit >= 0; bit = bitset.nextSetBit(bit+1)) {
		for (int tid=itemsetObject.tidset.nextSetBit(0); tid >= 0; tid = itemsetObject.tidset.nextSetBit(tid+1)) {
			hashcode += tid;
	    }
		return (hashcode % size);
	}
}
