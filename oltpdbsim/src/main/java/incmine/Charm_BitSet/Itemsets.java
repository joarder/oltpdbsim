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

public class Itemsets {

	private final List<List<Itemset>> levels = new ArrayList<List<Itemset>>();  // itemset class par taille
	private int itemsetsCount=0;
	
	public Itemsets() {
		levels.add(new ArrayList<Itemset>()); // We create an empty level 0 by default.
	}
			
	public void addItemset(Itemset itemset) {
		
		while(levels.size() <= itemset.itemset.size()) {
			levels.add(new ArrayList<Itemset>());
		}
	
		levels.get(itemset.itemset.size()).add(itemset);
		itemsetsCount++;
	}

	public List<List<Itemset>> getLevels() {
		return levels;
	}

	public int getItemsetsCount() {
		return itemsetsCount;
	}	
}