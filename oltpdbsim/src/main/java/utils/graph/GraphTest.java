package main.java.utils.graph;

import java.util.HashSet;
import java.util.Set;

public class GraphTest {

	public static void main(String[] args) {
		SimpleHypergraph<SimpleVertex, SimpleHEdge> hgr = new SHypergraph<SimpleVertex, SimpleHEdge>();
		
		SimpleVertex v1 = new SimpleVertex(1, 1);
		hgr.addVertex(v1);
		SimpleVertex v2 = new SimpleVertex(2, 1);
		hgr.addVertex(v2);
		SimpleVertex v3 = new SimpleVertex(3, 1);
		hgr.addVertex(v3);
		SimpleVertex v4 = new SimpleVertex(4, 1);
		hgr.addVertex(v4);
		SimpleVertex v5 = new SimpleVertex(5, 1);
		hgr.addVertex(v5);
		SimpleVertex v6 = new SimpleVertex(6, 1);
		hgr.addVertex(v6);
		SimpleVertex v7 = new SimpleVertex(7, 1);
		hgr.addVertex(v7);
		SimpleVertex v8 = new SimpleVertex(8, 1);
		hgr.addVertex(v8);
		SimpleVertex v9 = new SimpleVertex(9, 1);
		hgr.addVertex(v9);
		SimpleVertex v10 = new SimpleVertex(10, 1);
		hgr.addVertex(v10);

		SimpleHEdge h1 = new SimpleHEdge(1, 1);
		Set<SimpleVertex> vSet1 = new HashSet<SimpleVertex>();
		vSet1.add(v1);
		vSet1.add(v2);
		vSet1.add(v7);
		hgr.addHEdge(h1, vSet1);
		
		SimpleHEdge h2 = new SimpleHEdge(2, 1);
		Set<SimpleVertex> vSet2 = new HashSet<SimpleVertex>();
		vSet2.add(v1);
		vSet2.add(v2);
		vSet2.add(v3);
		vSet2.add(v5);
		vSet2.add(v6);
		hgr.addHEdge(h2, vSet2);
		
		SimpleHEdge h3 = new SimpleHEdge(3, 1);
		Set<SimpleVertex> vSet3 = new HashSet<SimpleVertex>();
		vSet3.add(v1);
		vSet3.add(v2);
		vSet3.add(v5);
		vSet3.add(v8);
		hgr.addHEdge(h3, vSet3);
		
		SimpleHEdge h4 = new SimpleHEdge(4, 1);
		Set<SimpleVertex> vSet4 = new HashSet<SimpleVertex>();
		vSet4.add(v3);
		vSet4.add(v4);
		vSet4.add(v5);
		vSet4.add(v9);
		hgr.addHEdge(h4, vSet4);
		
		SimpleHEdge h5 = new SimpleHEdge(5, 1);
		Set<SimpleVertex> vSet5 = new HashSet<SimpleVertex>();
		vSet5.add(v3);
		vSet5.add(v6);
		vSet5.add(v10);
		hgr.addHEdge(h5, vSet5);
		
		System.out.println("Initial state >> ");
		for(SimpleVertex v: hgr.getVertices()) {
			System.out.println(v.toString()+" | "+hgr.getNeighbors(v)+" | Incident Edges = "+hgr.getIncidentEdges(v));
		}
		
		System.out.println("Before >> ");
		show(hgr);
		
		// Remove h5
		hgr.removeHEdge(h5);
		
		System.out.println("After >> ");
		show(hgr);
	}

	private static void show(SimpleHypergraph<SimpleVertex, SimpleHEdge> hgr) {
		int edges = 0;		
		int vertices = hgr.getVertexCount();
		
		String content = "";		
		for(SimpleVertex v : hgr.getVertices()) {

			String str = Integer.toString(v.getWeight())+" ";
			
			for(SimpleVertex _v : hgr.getNeighbors(v)) {
				if(!v.equals(_v)) {
					SimpleHEdge _h = hgr.findEdge(v, _v);
					
					++edges;
					
					String _id = Integer.toString(_v.getId());
					String _edge_weight = Integer.toString(_h.getWeight());
					
					str += _id+" "+_edge_weight+" ";
				}
			}

			str += "\n";
			content += str;
		}
		
		System.out.println("Vertices = "+vertices+" | Edges = "+edges);
		System.out.println(content);
	}
}
