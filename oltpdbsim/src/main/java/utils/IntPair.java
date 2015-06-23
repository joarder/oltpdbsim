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

package main.java.utils;

public class IntPair {
    public int x, y;
    
    public IntPair(int x, int y) {
        this.x = x;
        this.y = y;
    }
    
    @Override
    public int hashCode() {
    	 int result = x;
         result = 31 * result + y;
         return result;
    }
 
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof IntPair)) 
        	return false;
        
        IntPair pair = (IntPair) object;
        return ((pair.x == x && pair.y == y) || (pair.x == y && pair.y == x));            
    }

    @Override
    public String toString() {
        return "("+x+","+y+")";
    }
}