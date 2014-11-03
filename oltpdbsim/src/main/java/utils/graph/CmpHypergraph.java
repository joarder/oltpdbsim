package main.java.utils.graph;

import java.io.Serializable;
import java.util.Set;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class CmpHypergraph<V extends CompressedVertex, H  extends CompressedHEdge> 
	extends SimHypergraph<V, H> 
	implements CompressedHypergraph<V, H>, Serializable {	
	
	@Override
	public boolean addCHEdge(H h, Set<V> vSet) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public H getCHEdge(int id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public H getCHEdge(Set<V> vSet) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public V getCVertex(int id) {

		for(Entry<V, Set<H>> entry : this.vertices.entrySet()) {
			if(entry.getKey().getId() == id)
				return entry.getKey();
		}
		
		return null;
	}

}