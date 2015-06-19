package main.java.utils.graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.uci.ics.jung.graph.SetHypergraph;
import main.java.cluster.Cluster;
import main.java.entry.Global;
import main.java.utils.Utility;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

@SuppressWarnings("serial")
public class SimHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
	extends SetHypergraph<V, H> 
	implements ISimpleHypergraph<V, H>, Serializable {
	
    protected Map<Integer, V> vMap;
    protected Map<Integer, H> hMap;
	
    protected Map<CompressedHEdge, Set<CompressedVertex>> cHEdges;
    protected Map<CompressedVertex, Set<CompressedHEdge>> cVertices;
    
    protected Map<Integer, CompressedHEdge> cHEMap;
    protected Map<Integer, CompressedVertex> cVMap; 
	
	public SimHypergraph() {
		        
		this.vMap = new HashMap<Integer, V>();
		this.hMap = new HashMap<Integer, H>();
		
		this.cHEdges = new HashMap<CompressedHEdge, Set<CompressedVertex>>();
		this.cVertices = new HashMap<CompressedVertex, Set<CompressedHEdge>>();
				
		this.cHEMap = new HashMap<Integer, CompressedHEdge>();
		this.cVMap = new HashMap<Integer, CompressedVertex>();
	}

//===================================================================================================
	// Returns the vertices of a given hyperedge
	public Set<V> getVertices(H h) {
		return edges.get(h.getId());
	}
	
	// Adds a new Hyperedge in the Hypergraph
	public boolean addHEdge(H h, Set<V> vSet) {        
		
		if (edges.containsKey(h))
			return false;
		
		this.edges.put(h, vSet);
		this.hMap.put(h.getId(), h);
		
        for (V v : vSet) {
            // add v if it's not already in the graph
        	this.addVertex(v);
        	
            this.vMap.put(v.getId(), v);
            
            // associate v with hyperedge
            vertices.get(v).add(h);
            this.updateVertexWeight(v);
        }
        
        if(Global.compressionEnabled)
        	this.addCHEdge(h);
        
        return true;
    }
	
	// Removes a hyperedge from the Hypergraph	
	public boolean removeHEdge(H h) {
		
		if (!this.containsEdge(h))
            return false;
		
		Set<V> toBeRemoved = new HashSet<V>();
        for (V v : edges.get(h)) {
            this.vertices.get(v).remove(h);            
            
            if(this.getNeighborCount(v) == 0 && this.getIncidentEdges(v).isEmpty()) {
            	this.vMap.remove(v.getId());
            	toBeRemoved.add(v);
            	
            } else {
            	this.updateVertexWeight(v);
            }
        }
	
        this.hMap.remove(h.getId());
        this.edges.remove(h);        

        for(V _v : toBeRemoved) {
        	this.removeVertex(_v);
        	
        	if(Global.compressionEnabled)
        		this.removeCVertex(_v);
        }
        	
        //System.out.println("--> Removing "+h.getId());
		if(Global.compressionEnabled)
        	this.removeCHEdge(h);
		
		return true;
	}
		
	// Returns a Hyperedge based on the given id
	public H getHEdge(int key) {
		return this.hMap.get(key);
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
		return this.vMap.get(key);
	}
	
	// Updating hyperedge and compressed hyperedge weights
	public void updateHEdgeWeight(H h, int weight) {
		h.setWeight(weight);
				
        // Updating vertex weight
		for(V v : edges.get(h))
        	this.updateVertexWeight(v);
		
		if(Global.compressionEnabled) {
			// Updating Compressed Hyperedge Weight
			CompressedHEdge ch = this.getCHEdge(h);
			ch.updateWeight();
			
			// Updating Compressed Vertex Weight
			for(CompressedVertex cv : getIncidentCVertices(ch))
				cv.updateWeight();							
		}
	}
	
	// Updating Vertex weight
	public void updateVertexWeight(V v) {
		int weight = 0;
		
		for(SimpleHEdge h : this.getIncidentEdges(v))
			weight += h.getWeight();		
			
		v.setWeight(weight);	
	}

//===================================================================================================
	// Adds a new Compressed Hyperedge in the Compressed Hypergraph if necessary
	public boolean addCHEdge(H h) {
		
		Set<CompressedVertex> cvSet = new HashSet<CompressedVertex>();
		
		for(V v : getIncidentVertices(h)) {
            // Add v into a Compressed Vertex, creates a new one if necessary
			this.addCVertex(v);
			cvSet.add(getCVertex(v));
		}
		
		// Check whether a Compressed Hyperedge already exists for this set of compressed vertices
		// If exists then add this Hyperedge h into it
		// Otherwise create a new Compressed Hyperedge
		CompressedHEdge ch = this.getCHEdge(cvSet);
		
		if(ch != null) {			
			//ch.incWeight(h.getWeight());
			ch.getHESet().put(h.getId(), h);			
			ch.updateWeight();
			
			// Add incident Compressed Vertices
			this.cHEdges.get(ch).addAll(cvSet);
			
			// Add incident Compressed Hyperedge
			for(CompressedVertex cv : cvSet)
				this.cVertices.get(cv).add(ch);
			
			//System.out.println(ch);
			return false;
			
		} else {
			CompressedHEdge new_ch = new CompressedHEdge(++Global.cHEdgeSeq, h.getWeight());
			new_ch.getHESet().put(h.getId(), h);

			// Add incident Compressed Vertices
			this.cHEdges.put(new_ch, new HashSet<CompressedVertex>(cvSet));
			this.cHEMap.put(new_ch.getId(), new_ch);
						
			// Add incident Compressed Hyperedge
			for(CompressedVertex cv : cvSet)
				this.cVertices.get(cv).add(new_ch);
			
			//System.out.println(new_ch);
			return true;
		}
	}	
		
	// Removes a Compressed Hyperedge in the Compressed Hypergraph if necessary
	public boolean removeCHEdge(H h) {		
		CompressedHEdge ch = getCHEdge(h);
		//System.out.println("\t--> Removing "+ch.toString());
		ch.getHESet().remove(h.getId());
		
		if(ch.getHESet().isEmpty()) {
			//System.out.println("\t\t--> Removing empty "+ch.toString());
			//System.out.println("\t\t--> Incident CVertices: "+this.cHEdges.get(ch));
			
			// Removing from incident compressed hyperedges
			for(CompressedVertex cv : this.getIncidentCVertices(ch)){
				//System.out.println("\t\t\t--> HSet["+cv.toString()+"] :: "+this.cVertices.get(cv));
				this.cVertices.get(cv).remove(ch);
			}
			
			this.cHEdges.remove(ch);
			this.cHEMap.remove(ch.getId());			
			
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
		
		String[] parts = Cluster.getTplIdFromDataId(v.getId());
		int tpl_pk = Integer.parseInt(parts[0]);
		
		int cv_id = Utility.simpleHash(tpl_pk, Global.virtualDataNodes);		
		CompressedVertex cv = this.getCVertex(cv_id);
		
		if(cv != null) {			
			if(!cv.getVSet().containsKey(v.getId())) {
				cv.getVSet().put(v.getId(), v);
				cv.updateWeight();
			}

			//System.out.println(cv);
			return false;
			
		} else {			
			CompressedVertex new_cv = new CompressedVertex(cv_id, v.getWeight(), v.getPartition_id(), v.getServer_id());	        	
	        new_cv.getVSet().put(v.getId(), v);	    
	        
        	this.cVertices.put(new_cv, new HashSet<CompressedHEdge>());
        	this.cVMap.put(new_cv.getId(), new_cv);
        	
        	//System.out.println(new_cv);
        	return true;
		}		
	}
	
	// 
	public boolean removeCVertex(V v) {		
		CompressedVertex cv = getCVertex(v);
		//System.out.println("\t--> Removing "+v.toString()+" from "+cv.toString()+"|"+cv.getWeight());
		cv.getVSet().remove(v.getId());

		if(cv.getVSet().isEmpty()) {
			//System.out.println("\t\t--> Removing empty "+cv.toString());
			//System.out.println("\t\t--> Incident CHEdges: "+this.cVertices.get(cv));
			
			// Removing from incident compressed hyperedges
			for(CompressedHEdge ch : this.getIncidentCHEdges(cv)){
				//System.out.println("\t\t\t--> VSet["+ch.toString()+"] :: "+this.cHEdges.get(ch));
				this.cHEdges.get(ch).remove(cv);
			}
			
			this.cVertices.remove(cv);
			this.cVMap.remove(cv.getId());
			
			return true;
			
		} else {
			//System.out.println("--> Decreasing weight ...");
			cv.updateWeight();
			//System.out.println("\t\t New weight: "+cv.toString()+"|"+cv.getWeight());
			return false;
		}
	}
	
	// Returns a Compressed Vertex based on the given id
	public CompressedVertex getCVertex(int cv_id) {
		return this.cVMap.get(cv_id);
	}	

	// Returns a Compressed Vertex based on the given Vertex
	public CompressedVertex getCVertex(V v) {
		
		for(Entry<CompressedVertex, Set<CompressedHEdge>> entry : this.cVertices.entrySet())
			if(entry.getKey().getVSet().containsKey(v.getId()))
				return entry.getKey();
		
		return null;
	}
	
	// Returns a Compressed Hyperedge based on the given id
	public CompressedHEdge getCHEdge(int ch_id) {
		return this.cHEMap.get(ch_id);
	}	
	
	// Returns a Compressed Hyperedge based on the given Hyperedge
	public CompressedHEdge getCHEdge(H h) {
		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.cHEdges.entrySet())
			if(entry.getKey().getHESet().containsKey(h.getId()))
				return entry.getKey();
		
		return null;
	}	

	// Returns a Compressed Hyperedge if it contains a given set of Compressed vertices
	public CompressedHEdge getCHEdge(Set<CompressedVertex> cvSet) {
		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.cHEdges.entrySet()) {
			if(entry.getValue().containsAll(cvSet))
				return entry.getKey();
		}
		
		return null;
	}

	// Returns all compressed hyeredges
	public Map<CompressedHEdge, Set<CompressedVertex>> getcHEdges() {
		return this.cHEdges;
	}
	
	// Returns all compressed vertices 
	public Map<CompressedVertex, Set<CompressedHEdge>> getcVertices() {
		return this.cVertices;
	}
	
	// Returns the set of compressed hyperedges covered by the given compressed vertex cv
	public Set<CompressedHEdge> getIncidentCHEdges(CompressedVertex cv) {
		return this.cVertices.get(cv);
	}
	
	// Returns the set of compressed vertices covered by the given compressed hyperedge ch
	public Set<CompressedVertex> getIncidentCVertices(CompressedHEdge ch) {
		return this.cHEdges.get(ch);
	}
	
	// Returns the set of Partitions covered by the given Hyperedge h
	public Set<Integer> getIncidentPartitions(WorkloadBatch wb, CompressedHEdge ch) {
		
		Set<Integer> incidentPartitions = new HashSet<Integer>();
		
		for(Entry<Integer, SimpleHEdge> entry : ch.getHESet().entrySet()) {		
			Transaction tr = wb.getTransaction(entry.getKey());
			incidentPartitions.addAll(tr.getTr_partitionSet().keySet());
		}
		
		return incidentPartitions;
	}
	
	// Returns the set of Servers covered by the given Hyperedge h
	public Set<Integer> getIncidentServers(WorkloadBatch wb, CompressedHEdge ch) {	
		
		Set<Integer> incidentServers = new HashSet<Integer>();
		
		for(Entry<Integer, SimpleHEdge> entry : ch.getHESet().entrySet()) {		
			Transaction tr = wb.getTransaction(entry.getKey());
			incidentServers.addAll(tr.getTr_serverSet().keySet());
		}
		
		return incidentServers;
	}
}