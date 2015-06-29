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

package main.java.utils.graph;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import edu.uci.ics.jung.graph.SetHypergraph;
import main.java.entry.Global;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class SimpleHypergraph<V extends SimpleVertex, H extends SimpleHEdge> 
	extends SetHypergraph<V, H> 
	implements ISimpleHypergraph<V, H> {

	private static final long serialVersionUID = 1L;

	protected Map<Integer, V> vMap;
    protected Map<Integer, H> hMap;
	
    public Map<CompressedHEdge, Set<CompressedVertex>> cHEdges;
    public Map<CompressedVertex, Set<CompressedHEdge>> cVertices;
    
    public Map<Integer, CompressedHEdge> cHEMap;
    public Map<Integer, CompressedVertex> cVMap; 
	
	public SimpleHypergraph() {		        
		this.vMap = new HashMap<Integer, V>();
		this.hMap = new HashMap<Integer, H>();
	}

//===================================================================================================
	// Adds a new Hyperedge in the Hypergraph
	public boolean addHEdge(H h, Set<V> vSet) {		
		if (h == null)
            throw new IllegalArgumentException("input hyperedge may not be null");
        
        if (vSet == null)
            throw new IllegalArgumentException("endpoints may not be null");

        if(vSet.contains(null)) 
            throw new IllegalArgumentException("cannot add an edge with a null endpoint");
        
        Set<V> new_vSet = new HashSet<V>(vSet);
        
        if (edges.containsKey(h)) {
            Collection<V> attached = edges.get(h);
            
            if (!attached.equals(new_vSet)) {
                throw new IllegalArgumentException("Edge " + h + 
                        " exists in this graph with endpoints " + attached);
            } else {
                return false;
            }
        }
        
        hMap.put(h.getId(), h);
        edges.put(h, new_vSet);        
        
        for (V v : vSet) {        	
            this.addVertex(v);            
            vertices.get(v).add(h);            
            
            this.vMap.put(v.getId(), v);            	        	
            this.updateVertexWeight(v);
        }
        
        return true;
    }
	
	// Removes a hyperedge from the Hypergraph	
	public boolean removeHEdge(H h) {
		
		if (!containsEdge(h))
            return false;

		for (V v : edges.get(h)) {
            vertices.get(v).remove(h);
            
            if(this.getIncidentEdges(v).isEmpty()) {            	
            	this.vMap.remove(v.getId());
            	this.removeVertex(v);            	
            } else {
            	this.updateVertexWeight(v);
            }            
        }
		
		this.hMap.remove(h.getId());
        edges.remove(h);        
        
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
				
		/*for(V v : this.getIncidentVertices(h))
        	this.updateVertexWeight(v);*/
	}
	
	// Updating Vertex weight
	public void updateVertexWeight(V v) {
		/*int weight = 0;
			
		for(H h : this.getIncidentEdges(v))
			weight += h.getWeight();*/		
			
		//v.setWeight(this.getIncidentEdges(v).size());
		v.setWeight(1);
	}

//===================================================================================================
	// Adds a new Compressed Hyperedge in the Compressed Hypergraph if necessary
	public CompressedHEdge addCHEdge(H h) {
		//Global.LOGGER.info("--> Adding "+h.toString());
		Set<CompressedVertex> cvSet = new HashSet<CompressedVertex>();		
		for(V v : this.getIncidentVertices(h)) {            
			CompressedVertex cv = this.addCVertex(v);
			cvSet.add(cv);
			//Global.LOGGER.info("\t--> Added/Updated "+cv.toString()+" for "+v.toString());
			//cvSet.add(this.getCVMap().get(v.getCid()));
		}
		
		// Check whether a Compressed Hyperedge already exists for this set of compressed vertices
		// If exists then add this Hyperedge h into it
		// Otherwise create a new Compressed Hyperedge		
		CompressedHEdge ch = this.getCHEdge(cvSet);		
		
		if(ch != null) {
			ch.getHESet().put(h.getId(), h);			
			this.updateCHEdgeWeight(ch);
			
			// Add incident Compressed Vertices
			this.cHEdges.get(ch).addAll(cvSet);
			
			// Add incident Compressed Hyperedge
			for(CompressedVertex cv : cvSet)
				this.cVertices.get(cv).add(ch);
			
		} else {
			if(cvSet.size() >= 2) {
				ch = new CompressedHEdge(++Global.cHEdgeSeq, h.getWeight());
				ch.getHESet().put(h.getId(), h);
				this.updateCHEdgeWeight(ch);
	
				// Add incident Compressed Vertices
				this.cHEdges.put(ch, new HashSet<CompressedVertex>(cvSet));
				this.cHEMap.put(ch.getId(), ch);
							
				// Add incident Compressed Hyperedge
				for(CompressedVertex cv : cvSet)
					this.cVertices.get(cv).add(ch);
			}
		}
		
		return ch;
	}	
		
	// Removes a Compressed Hyperedge in the Compressed Hypergraph if necessary
	/*public boolean removeCHEdge(H h) {		
		CompressedHEdge ch = this.getCHEdge(h);
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
	}*/
	
	// Adds a Compressed Vertex in the Compressed Hypergraph if necessary
	public CompressedVertex addCVertex(V v) {
		// Check whether a Compressed Vertex already exists
		// If exists then add this Vertex v into it
		// Otherwise create a new Compressed Vertex
		
		CompressedVertex cv = this.cVMap.get(v.getCid());
		
		if(cv != null) {			
			if(!cv.getVSet().containsKey(v.getId())) {
				cv.getVSet().put(v.getId(), v);
				this.updateCVertexWeight(cv);
			}			
		} else {
			cv = new CompressedVertex(v.getCid(), v.getWeight(), v.getPartition_id(), v.getServer_id());	        	
	        cv.getVSet().put(v.getId(), v);
	        this.updateCVertexWeight(cv);
	        
        	this.cVertices.put(cv, new HashSet<CompressedHEdge>());
        	this.cVMap.put(cv.getId(), cv);
		}
		
		return cv;
	}
	
	// Removes or adjust a compressed vertex
	/*public boolean removeCVertex(V v) {		
		//CompressedVertex cv = this.getCVertex(v);
		CompressedVertex cv = this.getCVertex(v.getCid());
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
	}*/

	// Updating Compressed Hyperedge weight	
	public void updateCHEdgeWeight(CompressedHEdge ch) {
		/*int weight = 0;
		
		for(Entry<Integer, SimpleHEdge> h : HSet.entrySet()) {
			weight += h.getValue().getWeight();
		}
		
		ch.setWeight(weight);*/		
		ch.setWeight(ch.getHESet().size());		
	}
	
	
	// Updating Vertex weight
	public void updateCVertexWeight(CompressedVertex cv) {
		/*int weight = 0;
		
		for(CompressedHEdge ch : this.getIncidentCHEdges(cv))
			weight += ch.getWeight();				
		
		cv.setWeight(weight);*/
		//cv.setWeight(this.getIncidentCHEdges(cv).size());
		cv.setWeight(1);
	}
	
	// Returns a Compressed Vertex based on the given id
	/*public CompressedVertex getCVertex(int cv_id) {
		return this.cVMap.get(cv_id);
	}*/	

	// Returns a Compressed Vertex based on the given Vertex
	/*public CompressedVertex getCVertex(V v) {		
		for(Entry<CompressedVertex, Set<CompressedHEdge>> entry : this.cVertices.entrySet())
			if(entry.getKey().getVSet().containsKey(v.getId()))
				return entry.getKey();
		
		return null;
	}*/
	
	// Returns a Compressed Hyperedge based on the given id
	/*public CompressedHEdge getCHEdge(int ch_id) {
		return this.cHEMap.get(ch_id);
	}*/	
	
	// Returns a Compressed Hyperedge based on the given Hyperedge
	/*public CompressedHEdge getCHEdge(H h) {		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.cHEdges.entrySet())
			if(entry.getKey().getHESet().containsKey(h.getId()))
				return entry.getKey();
		
		return null;
	}*/	

	// Returns a Compressed Hyperedge if it contains a given set of Compressed vertices
	public CompressedHEdge getCHEdge(Set<CompressedVertex> cvSet) {		
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.cHEdges.entrySet())
			if(entry.getValue().containsAll(cvSet))
				return entry.getKey();		
		
		return null;
	}

	// Returns all compressed hyeredges
	public Map<CompressedHEdge, Set<CompressedVertex>> getCHEdgeMap() {
		return this.cHEdges;
	}
	
	// Setter
	public void setCHEdges(Map<CompressedHEdge, Set<CompressedVertex>> cHEdges) {
		this.cHEdges = cHEdges;
	}
	
	// Returns all compressed vertices 
	public Map<CompressedVertex, Set<CompressedHEdge>> getCVertexMap() {
		return this.cVertices;
	}
	
	// Setter
	public void setCVertices(Map<CompressedVertex, Set<CompressedHEdge>> cVertices) {		
		this.cVertices = cVertices; 
	}
	
	// Setter
	public void setCHEMap(Map<Integer, CompressedHEdge> cHEMap) {
		this.cHEMap = cHEMap;
	}	

	// Setter
	public void setCVMap(Map<Integer, CompressedVertex> cVMap) {
		this.cVMap = cVMap;
	}
	
	// Getter
	public Map<Integer, CompressedVertex> getCVMap() {
		return this.cVMap;
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
	
	public Set<CompressedHEdge> getCHEdges() {
		return this.cHEdges.keySet();
	}

	public Set<CompressedVertex> getCVertices() {
		return this.cVertices.keySet();
	}
	
	public Set<CompressedVertex> getCNeighbors(CompressedVertex cv) {
		if (!this.getCVertices().contains(cv))
            return null;
        
        Set<CompressedVertex> neighbors = new HashSet<CompressedVertex>();
        for (CompressedHEdge ch : this.getCVertexMap().get(cv)) {
            neighbors.addAll(this.getCHEdgeMap().get(ch));
        }
        
        return neighbors;
	}

	public CompressedHEdge findCEdge(CompressedVertex cv1, CompressedVertex cv2) {
		if (!this.getCVertices().contains(cv1) || !this.getCVertices().contains(cv2))
            return null;
        
        for (CompressedHEdge ch : this.getIncidentCHEdges(cv1)) {
            if (this.isCIncident(cv2, ch))
                return ch;
        }
        
        return null;
	}

	private boolean isCIncident(CompressedVertex cv, CompressedHEdge ch) {
		if (!this.getCVertices().contains(cv) || !this.getCHEdges().contains(ch))
            return false;
        
        return this.getCVertexMap().get(cv).contains(ch);
	}
}