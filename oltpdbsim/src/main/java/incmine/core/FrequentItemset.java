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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FrequentItemset {
    
    protected List<Integer> items = new ArrayList<Integer>();
    protected int support;
    protected int size;
    
    /***
     * Constructs a Frequent Itemset given a list of items and their support
     * @param items List of items
     * @param support Support value
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public FrequentItemset(List items, int support){
        this.items = items;
        this.support = support;
        this.size = this.items.size();
    }

    
    private List<FrequentItemset> getDirectSubsets(){
        List<FrequentItemset> res = new ArrayList<FrequentItemset>();
        for(int r=0;r<items.size();r++){
            List<Integer> subset = new ArrayList<Integer>(this.items);
            subset.remove(r);
            res.add(new FrequentItemset(subset,this.support));
        }
        return res;
    }

    @Override
    public boolean equals(Object obj){
        return this == obj ||
                (obj instanceof FrequentItemset &&
                ((FrequentItemset)obj).items.equals(this.items));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 71 * hash + (this.items != null ? this.items.hashCode() : 0);
        hash = 71 * hash + this.size;
        return hash;
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        sb.append(this.items.toString()).append(":").append(Integer.toString(this.support));
        return sb.toString();
    }
    
    /***
     * Extracts Frequent Itemsets from a set of Frequent Closed Itemsets
     * @param iterator Iterator of the list of Frequent Closed Itemsets
     * @param minSupport Minimum support threshold
     * @param segmentLength Number of transactions per segment
     * @return The list of frequent itemsets
     */
    public static List<FrequentItemset> getFIset(Iterator<SemiFCI> iterator, double minSupport, int segmentLength){
        
        List<ArrayList<FrequentItemset>> levels = new ArrayList<ArrayList<FrequentItemset>>();
        int last_size = -1;
        
        //every closed frequent itemset is a frequent itemset
        while(iterator.hasNext()){
            SemiFCI fci = iterator.next();
            FrequentItemset fi = new FrequentItemset(fci.getItems(), fci.getApproximateSupport(fci.getKValue()));
            
            if(fi.support >= Math.ceil(minSupport * segmentLength * (fci.getKValue() + 1))){
                if(last_size != fi.size){
                    levels.add(new ArrayList<FrequentItemset>());
                    last_size = fi.size;
                }
                levels.get(levels.size()-1).add(fi);
            }                               
        }
        
        //now for each fci generate fi by subsets generation
        for(int level = 0; level<levels.size()-1; level++){
            
            for(FrequentItemset fi:levels.get(level)){
                //generate subsets and check support
                for(FrequentItemset subFi:fi.getDirectSubsets()){
                    int index = levels.get(level+1).indexOf(subFi);
                    if(index == -1)
                        levels.get(level+1).add(subFi); //add new fi
                    else
                        levels.get(level+1).get(index).support =
                                Math.max(levels.get(level+1).get(index).support, subFi.support);
                }
            }
        }
        
        List<FrequentItemset> ret = new ArrayList<FrequentItemset>();
        for(List<FrequentItemset> level:levels)
            ret.addAll(level);
        
        return ret;
    }
    

}
