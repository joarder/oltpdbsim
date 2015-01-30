package main.java.cluster;

import java.util.SortedMap;
import java.util.TreeMap;
import main.java.utils.Utility;

public class ConsistentHashRing<T> {
	
  private final SortedMap<Integer, T> ring;
  //private final int replicas;

  public ConsistentHashRing(int replicas) {
    //this.replicas = replicas;
    this.ring = new TreeMap<Integer, T>();
  }

  // Add
  public void add(T node) {
    //for (int i = 1; i <= this.replicas; i++)
      //this.ring.put(Utility.md5Hash(node.toString() + i), node);    
      //this.ring.put(Utility.sha1Hash((node.toString() + i), false, false), node);
    	this.ring.put(Utility.intHash(Integer.parseInt(node.toString())), node);
  }

  // Remove
  public void remove(T node) {
    //for (int i = 1; i <= this.replicas; i++)
      //this.ring.remove(Utility.md5Hash(node.toString() + i));    
      //this.ring.remove(Utility.sha1Hash((node.toString() + i), false, false));
    	this.ring.remove(Utility.intHash(Integer.parseInt(node.toString())));
  }

  // Get
  public T get(int hashValue) {
    if (this.ring.isEmpty())
      return null;
    
    //Long hashValue = Utility.md5Hash(key);
    
    if (!this.ring.containsKey(hashValue)) {
      SortedMap<Integer, T> tailMap = this.ring.tailMap(hashValue);
      hashValue = tailMap.isEmpty() ? this.ring.firstKey() : tailMap.firstKey();
    }
    
    return this.ring.get(hashValue);
  }
}