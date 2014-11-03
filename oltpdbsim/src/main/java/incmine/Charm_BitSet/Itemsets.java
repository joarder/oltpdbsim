package main.java.incmine.Charm_BitSet;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/**
 * This class represents a set of itemsets.
 * They are ordered by size. For example, level 1 means itemsets of size 1 (that contains 1 item).
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
public class Itemsets implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 7931088952991324406L;
	private final List<List<Itemset>> levels = new ArrayList<List<Itemset>>();  // itemset class� par taille
	private int itemsetsCount=0;
	
	public Itemsets(){
		levels.add(new ArrayList<Itemset>()); // We create an empty level 0 by default.
	}
			
	public void addItemset(Itemset itemset){
		while(levels.size() <= itemset.itemset.size()){
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
