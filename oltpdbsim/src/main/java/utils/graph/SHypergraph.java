package main.java.utils.graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import edu.uci.ics.jung.graph.SetHypergraph;
import main.java.entry.Global;
import main.java.utils.Utility;

@SuppressWarnings("serial")
public class SHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
	extends SetHypergraph<V, H> 
	implements SimpleHypergraph<V, H>, Serializable {
	
    protected Map<Integer, V> vMap;
    protected Map<Integer, H> hMap;
	
    protected Map<CHEdge, Set<CVertex>> cHEdges;
    protected Map<CVertex, Set<CHEdge>> cVertices;
    
    protected Map<Integer, CVertex> cVMap;
    protected Map<Integer, CHEdge> cHEMap;
	
	public SHypergraph() {
		        
        vMap = new HashMap<Integer, V>();
        hMap = new HashMap<Integer, H>();
		
		cHEdges = new HashMap<CHEdge, Set<CVertex>>();
		cVertices = new HashMap<CVertex, Set<CHEdge>>();
		
		cVMap = new HashMap<Integer, CVertex>();
        cHEMap = new HashMap<Integer, CHEdge>();
	}

//===================================================================================================	
	// Adds a new Hyperedge in the Hypergraph
	public boolean addHEdge(H h, Set<V> vSet) {        
		
		if (edges.containsKey(h))
			return false;
		
		edges.put(h, vSet);
		hMap.put(h.getId(), h);
		
        for (V v : vSet) {
            // add v if it's not already in the graph
        	addVertex(v);
            vMap.put(v.getId(), v);
            
            // associate v with hyperedge
            vertices.get(v).add(h);
        }
        
        if(Global.compressionEnabled)
        	addCHEdge(h);
        
        return true;
    }
	
	// Removes a hyperedge from the Hypergraph
	public boolean removeHEdge(H h) {
		
		if (!containsEdge(h))
            return false;
		
		Set<V> toBeRemoved = new HashSet<V>();
        for (V v : edges.get(h)) {
            vertices.get(v).remove(h);
            
            if(getNeighborCount(v) == 0 && getIncidentEdges(v).size() == 0) {
            	vMap.remove(v.getId());
            	toBeRemoved.add(v);
            }
        }
	
        hMap.remove(h.getId());
        edges.remove(h);        

        for(V _v : toBeRemoved) {
        	removeVertex(_v);
        	
        	if(Global.compressionEnabled)
        		removeCVertex(_v);
        }
        	
		if(Global.compressionEnabled)
        	this.removeCHEdge(h);
		
		return true;
	}
		
	// Returns a Hyperedge based on the given id
	public H getHEdge(int key) {
		return hMap.get(key);
	}

	// Returns a Hyperedge based on the given set of Vertices
	public H getHEdge(Set<V> vSet) {
		
		for(Entry<H, Set<V>> entry : this.edges.entrySet()) {
			if(entry.getValue().containsAll(vSet))
				return entry.getKey();
		}
		
		return null;
	}

	// Returns a Vertex based on the given id
	public V getVertex(int key) {
		return vMap.get(key);
	}

//===================================================================================================
	// Adds a new Compressed Hyperedge in the Compressed Hypergraph if necessary
	public boolean addCHEdge(H h) {
		
		Set<CVertex> cvSet = new HashSet<CVertex>();
		
		for(V v : getIncidentVertices(h)) {
            // Add v into a Compressed Vertex, creates a new one if necessary
			addCVertex(v);
			cvSet.add(getCVertex(v));
		}
		
		// Check whether a Compressed Hyperedge already exists for this set of compressed vertices
		// If exists then add this Hyperedge h into it
		// Otherwise create a new Compressed Hyperedge
		CHEdge ch = this.getCHEdge(cvSet);
		
		if(ch != null) {			
			//ch.incWeight(h.getWeight());
			ch.getHESet().put(h.getId(), h);
			ch.updateWeight();
			
			// Add incident Compressed Vertices
			cHEdges.get(ch).addAll(cvSet);
			
			// Add incident Compressed Hyperedge
			for(CVertex cv : cvSet)
				cVertices.get(cv).add(ch);
			
			//System.out.println(ch);
			return false;
			
		} else {
			CHEdge new_ch = new CHEdge(++Global.cHEdgeSeq, h.getWeight());
			new_ch.getHESet().put(h.getId(), h);

			// Add incident Compressed Vertices
			cHEdges.put(new_ch, new HashSet<CVertex>(cvSet));
			cHEMap.put(new_ch.getId(), new_ch);
						
			// Add incident Compressed Hyperedge
			for(CVertex cv : cvSet)
				cVertices.get(cv).add(new_ch);
			
			//System.out.println(new_ch);
			return true;
		}
	}	
		
	// Removes a Compressed Hyperedge in the Compressed Hypergraph if necessary
	public boolean removeCHEdge(H h) {		
		CHEdge ch = getCHEdge(h);
		//System.out.println("--> "+ch.toString()+"|"+ch.getWeight());
		ch.getHESet().remove(h.getId());
		
		if(ch.getHESet().size() == 0) {
			//System.out.println("--> Removing "+ch.toString());
			cHEdges.remove(ch);
			cHEMap.remove(ch.getId());
			return true;
			
		} else { 
			//System.out.println("--> Decreasing weight ...");
			ch.updateWeight();
			//System.out.println("\t\t New weight: "+ch.toString()+"|"+ch.getWeight());
			return false;
		}
	}
	
	// Adds a Compressed Vertex in the Compressed Hypergraph if necessary
	public boolean addCVertex(V v) {
		// Check whether a Compressed Vertex already exists
		// If exists then add this Vertex v into it
		// Otherwise create a new Compressed Vertex
		
		int cv_id = Utility.simpleHash(v.getId(), Global.virtualNodes);		
		CVertex cv = this.getCVertex(cv_id);
		
		if(cv != null) {
			
			if(!cv.getVSet().containsKey(v.getId())) {
				cv.getVSet().put(v.getId(), v);
				//cv.incWeight(v.getWeight());
				cv.updateWeight();
			}

			//System.out.println(cv);
			return false;
			
		} else {			
			CVertex new_cv = new CVertex(cv_id, v.getWeight());	        	
	        new_cv.getVSet().put(v.getId(), v);	        
        	cVertices.put(new_cv, new HashSet<CHEdge>());
        	cVMap.put(new_cv.getId(), new_cv);
        	
        	//System.out.println(new_cv);
        	return true;
		}		
	}
	
	// 
	public boolean removeCVertex(V v) {
		CVertex cv = getCVertex(v);
		//System.out.println("--> "+cv.toString()+"|"+cv.getWeight());
		cv.getVSet().remove(v.getId());

		if(cv.getVSet().size() == 0) {
			//System.out.println("--> Removing "+cv.toString());
			cVertices.remove(cv);
			cVMap.remove(cv.getId());
			return true;
			
		} else {
			//System.out.println("--> Decreasing weight ...");
			cv.updateWeight();
			//System.out.println("\t\t New weight: "+cv.toString()+"|"+cv.getWeight());
			return false;
		}
	}
	
	// Returns a Compressed Vertex based on the given id
	public CVertex getCVertex(int key) {
		return cVMap.get(key);
	}	

	// Returns a Compressed Vertex based on the given Vertex
	public CVertex getCVertex(V v) {
		
		for(Entry<CVertex, Set<CHEdge>> entry : cVertices.entrySet())
			if(entry.getKey().getVSet().containsKey(v.getId()))
				return entry.getKey();
		
		return null;
	}
	
	// Returns a Compressed Hyperedge based on the given id
	public CHEdge getCHEdge(int key) {
		return cHEMap.get(key);
	}	
	
	// Returns a Compressed Hyperedge based on the given Hyperedge
	public CHEdge getCHEdge(H h) {
		
		for(Entry<CHEdge, Set<CVertex>> entry : cHEdges.entrySet())
			if(entry.getKey().getHESet().containsKey(h.getId()))
				return entry.getKey();
		
		return null;
	}	

	// Returns a Compressed Hyperedge if it contains a given set of Compressed vertices
	public CHEdge getCHEdge(Set<CVertex> cvSet) {
		
		for(Entry<CHEdge, Set<CVertex>> entry : cHEdges.entrySet()) {
			if(entry.getValue().containsAll(cvSet))
				return entry.getKey();
		}
		
		return null;
	}

	// 
	public Map<CHEdge, Set<CVertex>> getcHEdges() {
		return cHEdges;
	}	
}