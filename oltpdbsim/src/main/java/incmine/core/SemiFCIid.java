/*
 *    SemiFCIid.java
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
