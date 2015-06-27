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

public class FCITable implements Iterable<SemiFCI> {

	protected class FCIArray implements Iterable<SemiFCI> {

        private ArrayList<SemiFCI> itemsets;
        private LinkedList<Integer> garbageQueue;
        private ArrayList<Integer> goodPositions; //we store here the positions in the array containing a semiFCI to speedup iteration over the FCIArray
        
        private int size; //local variable to store the real number of semiFCIs stored
        
        /**
        * Default constructor. Constructs an istance of FCIarray with an empty set
        * of itemstes and an empty garbage queue.
        */
        public FCIArray() {
            this.itemsets = new ArrayList<SemiFCI>();
            this.garbageQueue = new LinkedList<Integer>();
            this.goodPositions = new ArrayList<Integer>();
            this.size = 0;
        }

        /**
        * Add an itemset to the FCIArray and returns the ID assigned.
        * 
        * @param itemset itemset to be added
        * @return id assigned to the itemset
        */
        public SemiFCIid addSemiFCI(SemiFCI itemset) {
            int pos;

            if(!this.garbageQueue.isEmpty()){
                pos = garbageQueue.poll();
                itemset.getId().setPosition(pos);
                this.itemsets.set(pos,itemset);
            }else{
                pos = this.itemsets.size();
                itemset.getId().setPosition(pos);
                this.itemsets.add(itemset);
            }

            this.size++;
            this.goodPositions.add(new Integer(pos));
//            this.goodPositionsIsSorted = false;
            
            return itemset.getId();
        }

        /**
        * Remove a semiFCI from the table by putting it in the garbage queue.
        * @param position id of the semiFCI in the array
        */
        public void removeSemiFCI(int position) {
            //add this position to the garbage queue
            //if(!this.garbageQueue.contains(position))
                this.garbageQueue.add(position);
                this.goodPositions.remove(new Integer(position));
                this.itemsets.set(position, null); //free up memory associated to ne removed semiFCI
            this.size--;
        }

        /**
        * Returns a copy of the semiFCI in the passed position
        *
        * @param position position of the semiFCI in the FCIArray
        * @return semiFCI in the desired position
        */
        public SemiFCI getFCI(int position) {

            if(!this.garbageQueue.contains(position))
                return this.itemsets.get(position);
            else
                return null;
        }

        /**
        * Size of the FCIArray
        * @return the number of SemiFCI in the FCIArray
        */
        public int size(){
            return this.size;
        }

        @Override
        public String toString() {

            StringBuilder sb = new StringBuilder();

            for(SemiFCI fci:this.itemsets){
                sb.append(fci.toString());
                sb.append("\n");
            }
            return sb.toString();
        }

        public Iterator<SemiFCI> iterator() {
            return new FCIArray.FCIArrayIterator();
        }

        class FCIArrayIterator implements Iterator<SemiFCI> {

//            private Iterator<SemiFCI> it = (Iterator<SemiFCI>) itemsets.iterator();
            private Iterator<Integer> goodPosIterator = goodPositions.iterator();
            private int currentPosition;
//            private boolean hasNextElement = false;
//            private SemiFCI nextElement;

            public boolean hasNext() {
//                checkForNextElement();
//                return this.hasNextElement;
                return this.goodPosIterator.hasNext();
            }

            public SemiFCI next() {
                if(!this.hasNext()) throw new NoSuchElementException();
//                return this.nextElement;
                currentPosition = this.goodPosIterator.next();
                return itemsets.get(currentPosition);
            }

            public void remove() {
                this.goodPosIterator.remove();
                garbageQueue.add(currentPosition);
                size--;
            }

        }
    }
    
    private HashMap<Integer, FCIArray> table;
    private InvertedFCIIndex invertedIndex;
    private int maxFCISize;
    
    public int nAdded = 0;
    public int nRemoved = 0;

    /**
     * Default constructor. Creates a new FCITable with an empty Inverted Index
     * associated.
     */
    public FCITable(){
        this.maxFCISize = 0;
        this.table = new HashMap<Integer, FCIArray>();
        this.invertedIndex = new InvertedFCIIndex();
    }

    /**
     * Add an itemset to the FCITable and returns the assigned ID
     * @param itemset itemset to be added to the table
     * @return id assigned to the passed itemset
     */
    public SemiFCIid addSemiFCI(SemiFCI itemset){
        int itemsetSize = itemset.getId().getDimension();
        
        if(itemsetSize > this.maxFCISize)
            this.maxFCISize = itemsetSize;
        
        if(!this.table.containsKey(itemsetSize))
            this.table.put(itemsetSize, new FCIArray());
        
        this.table.get(itemsetSize).addSemiFCI(itemset);
        this.invertedIndex.addSemiFCI(itemset);
        
        this.nAdded++;
        
        return itemset.getId();        
    }

    /**
     * Removes a semiFCI from the table given the corresponding ID.
     * @param id id of the semiFCI to be removed
     */
    public void removeSemiFCI(SemiFCIid id) {
        
        this.invertedIndex.removeSemiFCI(getFCI(id));
        this.table.get(id.getDimension()).removeSemiFCI(id.getPosition());
        
        this.nRemoved++;
    }
    
    /**
     * Removes a semiFCI from the table given the corresponding ID (via iterator).
     * @param id id of the semiFCI to be removed
     */
    public void removeSemiFCI(SemiFCIid id, Iterator<SemiFCI> iter) {
        
        this.invertedIndex.removeSemiFCI(getFCI(id));
//        this.table.get(id.getDimension()).removeSemiFCI(id.getPosition());
        iter.remove();
        this.nRemoved++;
    }

    /**
     * Returns the instance of the semiFCI associated to the passed id.
     * @param id id of the semiFCI
     * @return Instance of the semiFCI if present in the table, null otherwise
     */
    
    public SemiFCI getFCI(SemiFCIid id) {
        return this.table.get(id.getDimension()).getFCI(id.getPosition());
    }

    /**
     * Passed a semiFCI id, computes the maximum k (>k1) for which the support greater
     * than the corresponding value in the minimum support vector.
     *
     * @param id id of the semiFCI
     * @param supVector minimum support vector
     * @param k1 starting value for k
     * @return maximum k value
     */
    public int computeK(SemiFCIid id, int[] supVector, int startK) {
        
        return this.table.get(id.getDimension()).getFCI(id.getPosition()).computeK(supVector, startK);

    }

    /**
     * Returns the ID in the FCI table of the passed itemset (-1 if no entry is found)
     * @param itemset itemset to be selected
     * @return id of the semiFCI with the passed itemset
     */
    public SemiFCIid select(List<Integer> itemset) {
        List<Integer> positionList = this.invertedIndex.join(itemset,itemset.size());
        if(positionList.isEmpty())
            return new SemiFCIid(itemset.size(),-1);
        else
            return new SemiFCIid(itemset.size(), positionList.get(0));
    }

    /**
     * Returns the ID of the Smallest semiFCI Superset of the passed itemset
     * @param itemset itemeset to be selected
     * @return id of the semiFCI that is SFS of the passed itemset
     */
//    public SemiFCIid selectSFS(SemiFCI fci, boolean checkSupport) {
//        
//        List<SemiFCIid> sfsList = this.invertedIndex.selectSFS(fci.items);
//        
//        if(sfsList.isEmpty())
//            return new SemiFCIid (-1,-1);
//        else{
//            if(!checkSupport)
//                return sfsList.get(0);
//            else{
//                //use support information to check closure property
//                for(SemiFCIid sfsId:sfsList){
//                    SemiFCI sfsCandidate = this.getFCI(sfsId);
//                    if(sfsCandidate.getApproximateSupport(sfsCandidate.getKValue())==
//                            fci.getApproximateSupport(sfsCandidate.getKValue()))
//                        return sfsId;
//                }
//            }
//        }
//    }
    
    
    public SemiFCIid selectSFS(SemiFCI fci, boolean checkSupport) {
       
        for(int sfsSize = fci.size()+1; sfsSize < this.maxFCISize; sfsSize++){
            List<Integer> sfsPosList = this.invertedIndex.join(fci.getItems(),sfsSize);
            
            if(!sfsPosList.isEmpty()){
                if(!checkSupport)
                    return new SemiFCIid(sfsSize, sfsPosList.get(0));
                else{
                    for(Integer pos:sfsPosList){
                        SemiFCI sfsCandidate = this.getFCI(new SemiFCIid(sfsSize, pos));
                        //use support information to check closure property
                        if(sfsCandidate.getApproximateSupport(sfsCandidate.getKValue())==
                            fci.getApproximateSupport(sfsCandidate.getKValue()))
                        return sfsCandidate.getId();
                    }
                }
            }
        }
        //no sfs found!
        return new SemiFCIid(-1,-1);
    }
    
        public SemiFCIid selectSFS(List<Integer> subSetItemset, List<Integer> superSetItemset){
        
        for(int sfsSize = subSetItemset.size()+1; sfsSize < superSetItemset.size(); sfsSize++){
            List<Integer> sfsPosList = this.invertedIndex.join(subSetItemset,sfsSize);
            
            for(Integer pos:sfsPosList){
                SemiFCI sfsCandidate = this.getFCI(new SemiFCIid(sfsSize, pos));
                //check intersection property necessary in UpdateSubsetNotInL procedure
                if(Utilities.intersect2orderedList(sfsCandidate.getItems(), subSetItemset).equals(subSetItemset))
                    return sfsCandidate.getId();
            }
        }
        //no sfs found!
        return new SemiFCIid(-1,-1);
    }
    /**
     * Prepare the table to be able to process the next segment
     */
    public void clearNewItemsetsTable() {
        this.invertedIndex.clearNewItemsetsTable();
    }
    
    /**
     * Size of the FCITable
     * @return the number of SemiFCI currently stored in the table
     */
    public int size(){
        int size = 0;
        for(FCIArray array:this.table.values())
            size += array.size();
        
        return size;
    }
    
    @Override
    public String toString(){        
        StringBuilder sb = new StringBuilder();

        for(SemiFCI fci:this){
            sb.append(fci.toString()).append("\n");
        }
        
        return sb.toString();
    }

    /**
     * Provides a size descending iterator of the semiFCIs stored in the table
     * at the moment.
     * 
     * @return semiFCIs size descending iterator
     */
    public Iterator<SemiFCI> iterator() {
        return new FCITableIterator();
    }

    class FCITableIterator implements Iterator<SemiFCI>{

        private ArrayList<Integer> keys;
        private Iterator<SemiFCI> currentIterator;
        private int currentKeyIndex;
        private boolean hasNextElement = false;

        public FCITableIterator() {
            
            this.keys = new ArrayList<Integer>(table.keySet());
            Collections.sort(keys);
            this.currentKeyIndex = keys.size()-1;
            if(this.currentKeyIndex>=0)
                this.currentIterator = (Iterator<SemiFCI>) table.get(keys.get(currentKeyIndex)).iterator();
            
        }

        public boolean hasNext() {
            checkForNextElement();
            return this.hasNextElement;
        }

        public SemiFCI next() {
            
            return this.currentIterator.next();
        }
        
        public void remove() {
            this.currentIterator.remove();
        }
        
        private void checkForNextElement()
        {
            if(this.currentIterator == null){
                this.hasNextElement = false;
                return;
            }
            if(this.currentIterator.hasNext())
                this.hasNextElement = true;
            else
            {
                this.currentKeyIndex--;
                this.hasNextElement = false;
                while(this.currentKeyIndex>=0)
                {
                    this.currentIterator = (Iterator<SemiFCI>) table.get(this.keys.get(this.currentKeyIndex)).iterator();
                    if(this.currentIterator.hasNext())
                    {
                        this.hasNextElement = true;
                        return;
                    }
                    this.currentKeyIndex--;
                }    
            }
        }
    }
}
