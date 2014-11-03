/*
 *    SlidingWindowManager.java
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
import java.util.List;
import java.util.Observable;
import weka.core.Instance;

public abstract class SlidingWindowManager extends Observable implements Serializable{
    
    private static final long serialVersionUID = 1L;
    protected Segment currentSegment;
    protected double minSupport;
    
    /**
     * Default constructor.
     */
    public SlidingWindowManager(double minSupport, int maxItemsetLength)
    {
        this.currentSegment = new Segment(minSupport, maxItemsetLength);
        this.minSupport = minSupport;
    }
    
    public abstract void addInstance(Instance inst);

    /**
     * Returns the last list of FCIs that have been computed.
     * @return list of FCIs
     */
    public List<SemiFCI> getFCI(){
        return currentSegment.getFCI();
    }

    /**
     * Notifies the IncMine instance associated to the segment manager
     */
    protected void notifyIncMine()
    {
        setChanged();
        notifyObservers(currentSegment.size());
        clearChanged();
    }
     
}
