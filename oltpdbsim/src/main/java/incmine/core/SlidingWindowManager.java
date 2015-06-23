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
