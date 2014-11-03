/*
 *    Segment.java
 *    Copyright (C) 2012 Universitat Politècnica de Catalunya
 *    @author Massimo Quadrana <max.square@gmail.com>
 *
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    You should have received a copy of the GNU General Public License
 *    along with this program; if not, write to the Free Software
 *    Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */

package main.java.incmine.core;

import main.java.entry.Global;
import main.java.incmine.Charm_BitSet.AlgoCharm_Bitset;
import main.java.incmine.Charm_BitSet.Context;
import main.java.incmine.Charm_BitSet.Itemset;
import main.java.incmine.Charm_BitSet.Itemsets;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import weka.core.Instance;
import weka.core.SparseInstance;

public class Segment implements Serializable{

    private static final long serialVersionUID = -5259948122890387234L;
    private Context context;
    private int MAX_ITEMSET_LENGTH;
    private double minSupport;
    
    /**
     * Default constructor. Creates a new empty segment.
     */
    public Segment(double minSupport, int maxItemsetLength) {        
        this.context = new Context();
        this.minSupport = minSupport;
        this.MAX_ITEMSET_LENGTH = maxItemsetLength;
    }

    /**
     * Adds a new itemset to the segment
     * @param itemset itemset to be added
     */
    public void addItemset(Instance instance) {
        context.addItemset(toItemset(new SparseInstance(instance)));
    }
    
    /**
     * Removes all itemsets stored in the segment.
     */
    public void clear() {
        context = new Context();
    }

    /**
     * Returns the length of the segment. (Useful in case variable lenght segments)
     * @return lenght of the segment
     */
    public int size() {
        return context.size();
    }
    
    /**
     * Return the list of FCIs mined in the current segment in size ascending order
     * @return list of FCIs
     */
    public List<SemiFCI> getFCI() {
                
        
        AlgoCharm_Bitset charmBitset = new AlgoCharm_Bitset();        
        Itemsets closedItemsets = charmBitset.runAlgorithm(context, minSupport, 1000000);
        
        //System.out.println("Compute FCIs:"+charmBitset.getExecTime()+"ms\n (CHARM-BITSET)");
        Global.LOGGER.info("Computing FCIs ...");
        Global.LOGGER.info("Total "+closedItemsets.getItemsetsCount()+" frequent closed data tuple sets have been found in the last segment using CHARM-BITSET.");
        
        List<SemiFCI> fciSet = new ArrayList<SemiFCI>();
       
        for(int levelIndex = 0; levelIndex < closedItemsets.getLevels().size(); levelIndex++){
            if (this.MAX_ITEMSET_LENGTH != -1 && levelIndex > this.MAX_ITEMSET_LENGTH)
                break;
            List<Itemset> level = closedItemsets.getLevels().get(levelIndex);
            for(Itemset itemset: level){
                SemiFCI newFci = new SemiFCI(new ArrayList<Integer>(itemset.getItems()),itemset.getAbsoluteSupport()); 
                fciSet.add(newFci);
            }
        }
        
        
        return fciSet;
    }
    
    /**
     * Get sparse itemset representation of the current binary intastance
     * @param inst current transaction instance
     * @return an itemset composed by the indices of the non-zero elements in the instance
     */    
    private Itemset toItemset(SparseInstance inst){
        Itemset itemset = new Itemset();
        
        for(int val = 0; val < inst.numValues(); val++){
            itemset.addItem(inst.index(val));
        }
        return itemset;
    }
}