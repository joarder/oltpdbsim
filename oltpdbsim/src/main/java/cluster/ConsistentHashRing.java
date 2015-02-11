package main.java.cluster;

import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHashRing<T> {
	
	private final SortedMap<Long, T> ring;
	private final int replicas;

	public ConsistentHashRing(int replicas) {
		this.replicas = replicas;
		this.ring = new TreeMap<Long, T>();
	}

	// Add
	public void add(T node) {
		for (int i = 1; i <= this.replicas; i++) {
			this.ring.put(Long.parseLong(node.toString()), node);    	
			//this.ring.put(Utility.sha1Hash(node.toString() + i), node);
			//this.ring.put(Utility.intHash(Integer.parseInt(node.toString())), node);
		}
  }

	// Remove
	public void remove(T node) {
		for (int i = 1; i <= this.replicas; i++)
			this.ring.remove(node.toString());    
			//this.ring.remove(Utility.sha1Hash(node.toString() + i));
			//this.ring.remove(Utility.intHash(Integer.parseInt(node.toString())));
	}

	// Get
	public T get(long hash) {
	  
		if (this.ring.isEmpty())
			return null;
    
		long key = -1;
		if (!this.ring.containsKey(hash)) {    	
			SortedMap<Long, T> tailMap = this.ring.tailMap(hash);  
			key = tailMap.isEmpty() ? this.ring.firstKey() : tailMap.firstKey();
			//System.out.println(">> key = "+key+" | hash = "+hash+" | "+tailMap+" | "+this.ring.firstKey());
		}
    
		return this.ring.get(key);
	}
}