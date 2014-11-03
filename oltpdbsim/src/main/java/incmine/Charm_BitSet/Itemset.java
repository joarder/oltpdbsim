package main.java.incmine.Charm_BitSet;

import java.io.Serializable;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * This class represents an itemset
 * 
 * Copyright (c) 2008-2012 Philippe Fournier-Viger
 * 
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 *
 * SPMF is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * SPMF is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with SPMF.  If not, see <http://www.gnu.org/licenses/>.
 */
public class Itemset implements Serializable{
        
        /**
	 * 
	 */
	private static final long serialVersionUID = -1490857156451018246L;
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
