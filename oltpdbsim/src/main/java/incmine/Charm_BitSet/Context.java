package main.java.incmine.Charm_BitSet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * This class represents a binary context. It can read the context directly from
 * a file. See the ca.pfv.spmf.test folder for some examples of files containing
 * binary contexts.
 * 
 * Copyright (c) 2008-2012 Philippe Fournier-Viger
 * 
 * This file is part of the SPMF DATA MINING SOFTWARE
 * (http://www.philippe-fournier-viger.com/spmf).
 * 
 * SPMF is free software: you can redistribute it and/or modify it under the
 * terms of the GNU General Public License as published by the Free Software
 * Foundation, either version 3 of the License, or (at your option) any later
 * version.
 * 
 * SPMF is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
 * A PARTICULAR PURPOSE. See the GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License along with
 * SPMF. If not, see <http://www.gnu.org/licenses/>.
 */
public class Context implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2881137224215367135L;
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
