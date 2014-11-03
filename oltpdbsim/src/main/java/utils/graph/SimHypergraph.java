package main.java.utils.graph;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import main.java.entry.Global;
import main.java.utils.Utility;
import edu.uci.ics.jung.graph.SetHypergraph;

@SuppressWarnings("serial")
public class SimHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
	extends SetHypergraph<V, H> 
	implements SimpleHypergraph<V, H>, Serializable {
	
	private Map<CompressedHEdge, Set<CompressedVertex>> cHEdges;
	private Map<CompressedVertex, Set<CompressedHEdge>> cVertices;
	
	public SimHypergraph() {
		
		this.setcHEdges(new TreeMap<CompressedHEdge, Set<CompressedVertex>>());
		this.setcVertices(new TreeMap<CompressedVertex, Set<CompressedHEdge>>());
	}
	
	public Map<CompressedHEdge, Set<CompressedVertex>> getcHEdges() {
		return cHEdges;
	}

	public void setcHEdges(Map<CompressedHEdge, Set<CompressedVertex>> cHEdges) {
		this.cHEdges = cHEdges;
	}

	public Map<CompressedVertex, Set<CompressedHEdge>> getcVertices() {
		return cVertices;
	}

	public void setcVertices(Map<CompressedVertex, Set<CompressedHEdge>> cVertices) {
		this.cVertices = cVertices;
	}
	
	// Returns a Compressed Hyperedge if it contains a given set of Compressed vertices
	public CompressedHEdge getCompressedHEdgeContainingCVSet(Set<CompressedVertex> cvSet) {
		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.getcHEdges().entrySet()) {
			if(entry.getValue().containsAll(cvSet))
				return entry.getKey();
		}
		
		return null;
	}
	
	// Adds a new Hyperedge in the Hypergraph
	public void addHEdge(H h, Set<V> vSet) {        
        
        edges.put(h, vSet);
        
        for (V v : vSet) {
            // add v if it's not already in the graph
            addVertex(v);
            
            // associate v with hyperedge
            vertices.get(v).add(h);
        }
        
        if(Global.compressionEnabled)
        	this.addCompressedHEdge(h);
    }
		
	// Adds a new Compressed Hyperedge in the Compressed Hypergraph if necessary
	public CompressedHEdge addCompressedHEdge(H h) {
		
		Collection<V> vSet = this.getIncidentVertices(h);
		Set<CompressedVertex> cvSet = new TreeSet<CompressedVertex>();
		
		for(V v : vSet) {
            // Add v into a Compressed Vertex, creates a new one if necessary
			CompressedVertex cv = this.addCompressedVertex(v);				
			cvSet.add(cv);
		}
		
		// Check whether a Compressed Hyperedge already exists for this set of compressed vertices
		// If exists then add this Hyperedge h into it
		// Otherwise create a new Compressed Hyperedge
		CompressedHEdge ch = this.getCompressedHEdgeContainingCVSet(cvSet);
		
		if(ch != null) {			
			ch.incWeight(h.getWeight());
			ch.getHESet().put(h.getId(), h);
			
			// Add incident Compressed Vertices
			this.getcHEdges().get(ch).addAll(cvSet);
			
			// Add incident Compressed Hyperedge
			for(CompressedVertex cv : cvSet)
				this.getcVertices().get(cv).add(ch);
			
			//System.out.println(ch);
			return ch;
			
		} else {
			CompressedHEdge new_ch = new CompressedHEdge(++Global.cHEdgeSeq, h.getWeight());
			new_ch.getHESet().put(h.getId(), h);

			// Add incident Compressed Vertices
			this.getcHEdges().put(new_ch, new TreeSet<CompressedVertex>(cvSet));
						
			// Add incident Compressed Hyperedge
			for(CompressedVertex cv : cvSet)
				this.getcVertices().get(cv).add(new_ch);
			
			//System.out.println(new_ch);
			return new_ch;
		}
	}
	
	// Adds a Compressed Vertex in the Compressed Hypergraph if necessary
	public CompressedVertex addCompressedVertex(V v) {
		// Check whether a Compressed Vertex already exists
		// If exists then add this Vertex v into it
		// Otherwise create a new Compressed Vertex
		
		int cv_id = Utility.simpleHash(v.getId(), Global.virtualNodes);		
		CompressedVertex cv = this.getCompressedVertex(cv_id);
		
		if(cv != null) {
			
			if(!cv.getVSet().containsKey(v.getId())) {
				cv.getVSet().put(v.getId(), v);
				cv.incWeight(v.getWeight());
			}

			//System.out.println(cv);
			return cv;
			
		} else {			
			CompressedVertex new_cv = new CompressedVertex(cv_id, v.getWeight());	        	
	        new_cv.getVSet().put(v.getId(), v);	        
        	this.getcVertices().put(new_cv, new TreeSet<CompressedHEdge>());        	
        	
        	//System.out.println(new_cv);
        	return new_cv;
		}		
	}
	
	// Returns a Compressed Vertex based on the given id
	public CompressedVertex getCompressedVertex(int id) {
		for(Entry<CompressedVertex, Set<CompressedHEdge>> entry : this.cVertices.entrySet()) {
			if(entry.getKey().getId() == id)
				return entry.getKey();
		}
		
		return null;
	}	

	// Returns a Compressed Vertex based on the given Vertex
	public CompressedVertex getCompressedVertexContainingV(V v) {
		
		for(Entry<CompressedVertex, Set<CompressedHEdge>> entry : this.getcVertices().entrySet())
			if(entry.getKey().getVSet().containsKey(v.getId()))
				return entry.getKey();
		
		return null;
	}
	
	// Returns a Compressed Hyperedge based on the given id
	public CompressedHEdge getCompressedHEdge(int id) {
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.cHEdges.entrySet()) {
			if(entry.getKey().getId() == id)
				return entry.getKey();
		}
		
		return null;
	}
	
	
	// Returns a Compressed Hyperedge based on the given Hyperedge
	public CompressedHEdge getCompressedHEdgeContainingH(H h) {
		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.getcHEdges().entrySet())
			if(entry.getKey().getHESet().containsKey(h.getId()))
				return entry.getKey();
		
		return null;
	}	
	
	// Returns a Hyperedge based on the given id
	public H getHEdge(int id) {

		for(H h : this.edges.keySet()) {
			if(h.getId() == id)
				return h;
		}
		
		return null;
	}

	// Returns a Hyperedge based on the given set of Vertices
	public H getHEdge(Set<V> vSet) {
		
		for(Entry<H, Set<V>> entry : this.edges.entrySet()) {
			if(entry.getValue().equals(vSet))
				return entry.getKey();
		}
		
		return null;
	}

	// Returns a Vertex based on the given id
	public V getVertex(int id) {

		for(V v : this.vertices.keySet()) {
			if(v.getId() == id)
				return v;
		}
		
		return null;
	}	
}