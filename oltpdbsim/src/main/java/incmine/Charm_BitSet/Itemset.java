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

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

public class Itemset {
        
	Set<Integer> itemset; // ordered
	int cardinality;
	BitSet tidset;

	public Itemset() {
		itemset = new HashSet<Integer>();
	}
        
	public double getRelativeSupport(int nbObject) {
		return ((double) cardinality) / ((double) nbObject);
	}
        
    public int getAbsoluteSupport() {
		return cardinality;
	}
        
    public Set<Integer> getItems() {
		return itemset;
	}
        
    public void addItem(Integer value) {
		itemset.add(value);
	}
        
    public void setCardinality(int cardinality){
        this.cardinality = cardinality;
    }
        
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
 
        for(Integer item : itemset)
            sb.append(item).append(" ");
        
        return sb.toString();
    }
}