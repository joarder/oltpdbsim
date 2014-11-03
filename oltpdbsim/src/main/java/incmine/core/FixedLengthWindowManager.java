/*
 *    FixedLengthWindowManager.java
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
import weka.core.Instance;

public class FixedLengthWindowManager extends SlidingWindowManager {

    private static final long serialVersionUID = -4548175425739509372L;
    private double segmentLenght;

    /**
     * Default constructor. Creates a new Fixed Lenght Window Manager; it will manage
     * segments of length equal to the passed value.
     * 
     * @param segmentLength lenght of each segment
     */
    public FixedLengthWindowManager(double minSupport, int maxPatternLength, int segmentLength) {
        super(minSupport, maxPatternLength);
        this.segmentLenght = segmentLength;
    }

    @Override
    public void addInstance(Instance transaction){
        currentSegment.addItemset(transaction);
        if(currentSegment.size() >= segmentLenght){
            Global.LOGGER.info("Updating FCI set ...");
            notifyIncMine();
            currentSegment.clear();
        }
    }
}