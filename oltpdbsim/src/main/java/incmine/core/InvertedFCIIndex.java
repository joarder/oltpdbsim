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

import java.util.*;

public class InvertedFCIIndex {
    
	protected class IDArrayList {

        protected Map<Integer, ArrayList<Integer>> map; //performs better than TreeSet
        private Map<Integer, Boolean> sortCheck; //use a flag to check sort invariant
        
        public IDArrayList() {
            map = new HashMap<Integer, ArrayList<Integer>>();
            sortCheck = new HashMap<Integer, Boolean>();
        }
        
        public void addID(int fciSize, int id) {
            if(!map.containsKey(fciSize)){
                map.put(fciSize, new ArrayList<Integer>());
                sortCheck.put(fciSize, Boolean.TRUE);
            }
            
            //sort check
            if(map.get(fciSize).size() > 0 && id < map.get(fciSize).get(map.get(fciSize).size()-1))
                sortCheck.put(fciSize, Boolean.FALSE);
            
            map.get(fciSize).add(id);
        }
        
        public void removeID(int fciSize, Integer id) {
            map.get(fciSize).remove(id);
        }
        
        public List<Integer> getIDArray(int fciSize) {            
            if(!sortCheck.get(fciSize)){
                Collections.sort(map.get(fciSize));
                sortCheck.put(fciSize, Boolean.TRUE);
            }
            
            if(map.containsKey(fciSize))                
                return map.get(fciSize);
            
            return null;
        }

        @Override
        public String toString(){
            StringBuilder sb = new StringBuilder();

            for(Integer key:map.keySet()){
                sb.append("{")
                        .append(key)
                        .append(":")
                        .append(map.get(key).toString())
                        .append("}");
            }
            return sb.toString();
        }
    }

    
    private Map<Integer, IDArrayList> invertedIndex;
    //stores newly added itemsets. we don't let them to be SFS of any semiFCI
    private Map<Integer, ArrayList<Integer>> newItemsets;

    /**
     * Default constructor. Creates a new empty instance of Inverted Index.
     */
    public InvertedFCIIndex() {        
        invertedIndex = new HashMap<Integer, IDArrayList>();
        newItemsets = new HashMap<Integer, ArrayList<Integer>>();
    }

    /**
     * Adds a new item with an empty IDArrayList.
     * @param item item to be added
     */
    public void addEmptyItem(Integer item) {        
        invertedIndex.put(item, new IDArrayList());
    }

    /**
     * Adds a semiFCI to the Inverted Index.
     * 
     * @param itemset semiFCI to be added
     */
    public void addSemiFCI(SemiFCI itemset) {

        for(Integer item:itemset.getItems()) {
            if(!invertedIndex.containsKey(item))
                this.addEmptyItem(item);

            invertedIndex.get(item).addID(itemset.getId().getDimension(), itemset.getId().getPosition());
        }

        //put this id into the newly added table
        if(!newItemsets.containsKey(itemset.getId().getDimension()))
            newItemsets.put(itemset.getId().getDimension(), new ArrayList<Integer>());
        
        newItemsets.get(itemset.getId().getDimension()).add(itemset.getId().getPosition());
    }

    /**
     * Removes a semiFCI from the index.
     * @param itemset semiFCI to be removed.
     */
    public void removeSemiFCI(SemiFCI itemset) {
        
        for(Integer item:itemset.getItems()) {
            if(invertedIndex.containsKey(item))
                invertedIndex.get(item).removeID(itemset.getId().getDimension(), itemset.getId().getPosition());
        }
    }

    /**
     * Finds the intersection of the IDarrays of dimension fciSize
     * associated to the items in the itemset passed.
     * 
     * @param itemset itemset to be joined
     * @param fciSize size associated to IDarrays that will be joined
     * @return common IDs
     */
    public List<Integer> join(List<Integer> itemset, final int fciSize)
    {
        for(Integer it:itemset) {
            if(!invertedIndex.containsKey(it) || !invertedIndex.get(it).map.containsKey(fciSize))
                return new ArrayList<Integer>(); //itemset not in L
        }
        
        List<Integer> intSet;
        if(itemset.size() == 1){
            intSet = invertedIndex.get(itemset.get(0)).getIDArray(fciSize);
        }else{
            //sort itemsets by IDArray length for intersection efficiency
            Collections.sort(itemset, new Comparator<Integer>(){
                public int compare(Integer o1, Integer o2) {
                    return invertedIndex.get(o1).map.get(fciSize).size()-
                            invertedIndex.get(o2).map.get(fciSize).size();
                }
            });

            Iterator<Integer> itemsIterator = itemset.iterator();
            intSet = Utilities.intersect2orderedList(invertedIndex.get(itemsIterator.next()).getIDArray(fciSize),
                    invertedIndex.get(itemsIterator.next()).getIDArray(fciSize));
            while(itemsIterator.hasNext() && !intSet.isEmpty())
                intSet = Utilities.intersect2orderedList(intSet,
                        invertedIndex.get(itemsIterator.next()).getIDArray(fciSize));
            
        }
        
        //in selecting SFS we remove newly added itemsets if any
        if(fciSize>itemset.size()) {
            Iterator<Integer> resIterator = intSet.iterator();
            while(resIterator.hasNext()) {
                int nextId = resIterator.next();
                if(newItemsets.containsKey(fciSize) && newItemsets.get(fciSize).contains(nextId))
                    resIterator.remove();
            }
        }
        
        return intSet;
        
    }
    
    /**
     * Clears the table filled with the set of new itemsets that have been found
     * during the last processing.
     */
    public void clearNewItemsetsTable(){
        newItemsets.clear();
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();
        for(Integer key:invertedIndex.keySet())
            sb.append(key)
                    .append("=>")
                    .append(invertedIndex.get(key).toString())
                    .append("\n");
        
        return sb.toString();
    }
}
