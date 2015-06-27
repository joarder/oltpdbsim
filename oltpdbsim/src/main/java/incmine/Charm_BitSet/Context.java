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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Context {
	// Contexte
	private final Set<Integer> attributes = new HashSet<Integer>();
	private final List<Itemset> objects = new ArrayList<Itemset>();

	int tidcount = 0;
        private int maxItemId = 0;

	public void addItemset(Itemset itemset) {
		objects.add(itemset);
		attributes.addAll(itemset.getItems());
                for(Integer item : itemset.getItems())
                    if (item > maxItemId)
                            maxItemId = item;
	}


	public void addObject(String attributs[]) {
		// We assume that there is no empty line
		Itemset itemset = new Itemset();
		// itemset.transactionId = tidcount++;
		for (String attribute : attributs) {
			int item = Integer.parseInt(attribute);
			itemset.addItem(item);
			attributes.add(item);
                        if (item > maxItemId)
                            maxItemId = item;
		}
		objects.add(itemset);
	}

	public int size() {
		return objects.size();
	}

	public List<Itemset> getObjects() {
		return objects;
	}

	public Set<Integer> getAttributes() {
		return attributes;
	}
        
        public int getMaxItemId(){
            return this.maxItemId;
        }
        
    @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();
            for(Itemset it : objects)
                sb.append(it).append("\n");
            
            return sb.toString();
        }

}
