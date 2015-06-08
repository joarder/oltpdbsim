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