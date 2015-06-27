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

package main.java.incmine.core;

import main.java.entry.Global;

import main.java.incmine.Charm_BitSet.AlgoCharm_Bitset;
import main.java.incmine.Charm_BitSet.Context;
import main.java.incmine.Charm_BitSet.Itemset;
import main.java.incmine.Charm_BitSet.Itemsets;

import java.util.ArrayList;
import java.util.List;

import weka.core.Instance;
import weka.core.SparseInstance;

public class Segment {

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