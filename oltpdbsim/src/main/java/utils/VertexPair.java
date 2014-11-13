package main.java.utils;

public class VertexPair {
    public int x, y;
    
    public VertexPair(int x, int y) {
        this.x = x;
        this.y = y;
    }
    
    @Override
    public int hashCode() {
        return x + y;
    }
 
    @Override
    public boolean equals(Object object) {
        if (!(object instanceof VertexPair)) 
        	return false;
        
        VertexPair pair = (VertexPair) object;
        return ((pair.x == x && pair.y == y) || (pair.x == y && pair.y == x));            
    }

    @Override
    public String toString() {
        return "("+x+","+y+")";
    }
}