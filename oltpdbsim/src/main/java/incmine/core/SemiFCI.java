/*
 *    SemiFCI.java
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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
//import moa.learners.IncMine;
import main.java.incmine.learners.IncMine;

public class SemiFCI implements Comparable<SemiFCI>,Serializable {

    private static final long serialVersionUID = 7468516413419567582L;
    private final List<Integer> items;
    private int[] supports;
    private boolean updated;
    private SemiFCIid id;
    private int k;

    /**
     * Default constructor. Creates a new semiFCI with the passed itemset and puts 
     * the current support at the head of the support vector.
     * @param item itemset of the semiFCI
     * @param currentSupport current support 
     */
    public SemiFCI(List<Integer> itemset, int currentSupport) {
        
        this.items = itemset;
        Collections.sort(itemset);
        
        this.id = new SemiFCIid(items.size(), -1);
        this.supports = new int[IncMine.windowSize];
        this.updated = true; //changed! newly added semiFCI are updated!!

        //append the current support at the beginning of the support vector
        this.supports[0] = currentSupport;
        this.k = 0;
    }

    /**
     * Returns the itemset of this semiFCI
     * @return the copy of the set of items stored
     */
    public List<Integer> getItems() {
        return new ArrayList<Integer>(items);
    }
    
    /**
     * The current value of k for k-SemiFCI
     * @return the current value of k
     */
    public int getKValue(){
        return this.k;
    }
    
    /**
     * 
     * @return the number of items stored
     */
    public int size(){
        return items.size();
    }
    
    public int compareTo(SemiFCI o) {
        return this.getId().getDimension()-o.getId().getDimension();
    }

    @Override
    public boolean equals(Object obj){
        return this == obj || 
                (obj instanceof SemiFCI 
                && this.items.equals(((SemiFCI)obj).items));
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (this.items != null ? this.items.hashCode() : 0);
        return hash;
    }
    /**
     * Returns the intersection of the itemsets of the current semiFCI and the passed one
     * @param o passed SemiFCI
     * @return ordered set of common items
     */
    public List<Integer> intersectWith(SemiFCI o) {
        return Utilities.intersect2orderedList(this.items, o.items);
    }

    /**
     * Compares the current itemset with the passed one
     * @param itemset itemset to compare
     * @return true if itemsets are equal, false otherwise
     */
    public boolean sameItemset(List<Integer> itemset) {
        return items.equals(itemset);
    }
    /**
     * @return the updated
     */
    public boolean isUpdated() {
        return updated;
    }

    /**
     * @param updated the updated to set
     */
    public void setUpdated(boolean updated) {
        this.updated = updated;
    }

    /**
     * @return the supports
     */
    public int[] getSupports() {
        return supports;
    }

    /**
     * @param supports the supports to set
     */
    public void setSupports(int[] supports) {
        this.supports = supports;
    }

    /**
     * @return the id
     */
    public SemiFCIid getId() {
        return id;
    }

    /**
     * @param id the id to set
     */
    public void setId(SemiFCIid id) {
        this.id = id;
    }

    /**
     * Compute the maximum k having support greater than the corresponding in the
     * minimum support vector
     *
     * @param supVector minimum support vector
     * @param k1 starting value for k
     * @return maximum k value
     */
    public int computeK(int[] supVector, int k1) {
        int maxK = -1;

        if(k1 < 0) return -1;

        for(int k = 0; k < supports.length; k++) {
            if(getApproximateSupport(k) >= supVector[k])
                maxK = k;
        }

        if(maxK != -1) {
            for(int i = maxK+1; i < supports.length; i++)
                supports[i] = 0;
        }
        this.k = maxK;
        return maxK;
    }

    /**
     * Push a support value in the head of the support vector
     * @param supValue value to be pushed
     */

    public void pushSupport(int supValue) {
        
        for(int i = supports.length-1; i > 0; i--)
            supports[i] = supports[i-1];

        supports[0] = supValue;
        setUpdated(true);
    }

    /**
     * Returns the current support of the semiFCI. If the semiFCI is not updated,
     * returns its support during the last segment that have been processed.
     * @return support value at the head of the support vector
     */
    public int currentSupport() {
        return supports[0];
    }

    /**
    * Computes the approximate support over the entire sliding window
    * 
    * @param k dimension the window
    * @return sum of the supports
    */
    public int getApproximateSupport(){
        return Utilities.cumSum(supports, supports.length-1);
    }
    
    /**
     * Computes the approximate support over a window of dimension k starting from
     * the current segment.
     * 
     * @param k dimension the window
     * @return sum of the supports
     */
    public int getApproximateSupport(int k) {
        return Utilities.cumSum(supports, k);
    }
    
    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();        
        sb.append(items.toString());

        sb.append("\t");
        for(int i = 0; i < (supports.length-1); i++) {
            sb.append(supports[i]);
            sb.append(",");
        }
        sb.append(supports[(supports.length-1)]);
        
        return sb.toString();
    }
}
