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

public class SemiFCIid implements Serializable{

    private static final long serialVersionUID = 4518952879185709365L;
    private int dimension;
    private int position;

    /**
     * Default constructor. Creates an new ID of a semiFCI, composed by the pair
     * (dimension, position)
     * @param dimension dimension of the semiFCI
     * @param position position of the semiFCI in the FCIArray associated to its dimension
     */
    public SemiFCIid(int dimension, int position) {
        this.dimension = dimension;
        this.position = position;
    }
    /**
     * @return the dimension
     */
    public int getDimension() {
        return dimension;
    }

    /**
     * @param dimension the dimension to set
     */
    public void setDimension(int dimension) {
        this.dimension = dimension;
    }

    /**
     * @return the position
     */
    public int getPosition() {
        return position;
    }

    /**
     * @param position the position to set
     */
    public void setPosition(int position) {
        this.position = position;
    }

    /**
     * Allows to check whether this identifier is valid. (If it's an identifier of 
     * a SemiFCI stored in the table)
     * @return true if it's a valid FCI identifier, false otherwise
     */
    public boolean isValid(){
        return position != -1;
    }
    
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("(").append(dimension).append(",").append(position).append(")");
        return sb.toString();
    }
}
