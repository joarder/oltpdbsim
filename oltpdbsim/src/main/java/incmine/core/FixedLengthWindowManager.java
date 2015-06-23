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