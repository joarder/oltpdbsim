/**
 * @author Joarder Kamal
 */

package main.java.repartition;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.utils.graph.CompressedHEdge;
import main.java.utils.graph.CompressedVertex;
import main.java.utils.graph.ISimpleHypergraph;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.WorkloadBatch;

public class WorkloadBatchProcessor {
	
	public static boolean generateWorkloadFile(Cluster cluster, WorkloadBatch wb) 
			throws IOException {
		
		boolean empty = false;

		// Workload file
		String wrl_file_name = Global.repartitioningCycle+"-"+Global.simulation; 
		String wrl_abs_file_name = Global.part_dir+Global.getRunDir()+wrl_file_name;				
		
		File workloadFile = new File(wrl_abs_file_name);
		
		wb.setWrl_file_name(wrl_abs_file_name);
		wb.setWrl_file(workloadFile);
		
		// Generates specified workload file for repartitioning
		switch(Global.workloadRepresentation) {
			case "hgr":
				if(Global.compressionEnabled)
					empty = generateCHGraphWorkloadFile(cluster, wb);
				else
					empty = generateHGraphWorkloadFile(cluster, wb, wb.hgr);			
				
				break;			
			
			case "gr":
				empty = generateGraphWorkloadFile(cluster, wb);			
				break;
		}
		
		return empty;
	}
	
	private static Map<Integer, Integer> vertex_id_map;
	private static Map<Integer, Integer> vvertex_id_map;
	
	// Generates Workload File for Hypergraph partitioning
	public static boolean generateHGraphWorkloadFile(Cluster cluster, WorkloadBatch wb, ISimpleHypergraph<SimpleVertex, SimpleHEdge> hgr) {
		
		vertex_id_map = new HashMap<Integer, Integer>();
		int vertex_id = 0;
		
		for(SimpleVertex v : hgr.getVertices()) {
			vertex_id_map.put(v.getId(), ++vertex_id);
						
			Data data = cluster.getData(v.getId());			
			data.setData_shadowId(vertex_id);
			data.setData_inUse(true);						
		}		
		
		int edges = hgr.getEdgeCount();		
		int vertices = hgr.getVertexCount();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;
		
		if(edges <= 1 ) {			
			Global.LOGGER.info("Only "+edges+" hyperedges present in the workload hypergraph network.");
			Global.LOGGER.info("Repartitioning will be aborted for this run ...");			
			return true;
			
		} else {		
			try {
				wb.getWrl_file().getParentFile().mkdirs();
				
				if(wb.getWrl_file().exists())
					wb.getWrl_file().delete();
				
				wb.getWrl_file().createNewFile();
				
				Writer writer = null;
				
				try {
					writer = new BufferedWriter(new OutputStreamWriter(
							new FileOutputStream(wb.getWrl_file()), "utf-8"));
					writer.write(edges+" "+vertices+" "+hasTransactionWeight+""+hasDataWeight+"\n");
					
					// Writing hyperedge weights and incident vertex ids
					for(SimpleHEdge e : hgr.getEdges()) {
						
						String e_weight = Integer.toString(e.getWeight());
						writer.write(e_weight+" ");
						
						Iterator<SimpleVertex> e_itr =  hgr.getIncidentVertices(e).iterator();
						while(e_itr.hasNext()) {
							String v_id = Integer.toString(vertex_id_map.get(e_itr.next().getId()));							
							writer.write(v_id);				
							
							if(e_itr.hasNext())
								writer.write(" "); 
						}
						
						writer.write("\n");
					}
					
					// Writing vertex weights
					Iterator<SimpleVertex> v_itr = hgr.getVertices().iterator();
					while(v_itr.hasNext()) {
						String v_weight = Integer.toString(v_itr.next().getWeight());							
						writer.write(v_weight);
						
						if(v_itr.hasNext())
							writer.write("\n"); 
					}										
					
				} catch(IOException e) {
					e.printStackTrace();
				}finally {
					writer.close();
				}
			} catch (IOException e) {		
				e.printStackTrace();
			}
			
			Global.LOGGER.info("Workload file generation for hypergraph based repartitioning has completed.");
			
			return false;
		}
	}
	
	// Generates Workload File for Compressed Hypergraph based repartitioning
	private static boolean generateCHGraphWorkloadFile(Cluster cluster, WorkloadBatch wb) {
		
		Map<CompressedHEdge, Set<CompressedVertex>> vedge = new HashMap<CompressedHEdge, Set<CompressedVertex>>();
		Set<CompressedVertex> vvertex = new HashSet<CompressedVertex>();
		
		Global.LOGGER.info("Total "+wb.hgr.getcHEdges().size()+" compressed hyperedges present in the current workload.");
		Global.LOGGER.info("Total "+wb.hgr.getcVertices().size()+" compressed vertices present in the current workload.");				
		Global.LOGGER.info("Only selecting the compressed hyperedges having at least two compressed vertices ...");

		// Only select the compressed hyperedges having at least two compressed vertices
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : wb.hgr.getcHEdges().entrySet()) {
			
			if(entry.getValue().size() >= 2) {				
				vedge.put(entry.getKey(), entry.getValue());
				vvertex.addAll(entry.getValue());
				
			} else {				
				Global.LOGGER.error("Compressed hyperedge with only 1 compressed vertex !!!");
			}
		}
				
		Global.LOGGER.info("Total "+vedge.size()+" compressed hyperedges spanning "+vvertex.size()+" compressed vertices are selected.");
		
		vvertex_id_map = new HashMap<Integer, Integer>();
		int vvertex_id = 0;
		
		for(CompressedVertex cv : vvertex) {
			vvertex_id_map.put(cv.getId(), ++vvertex_id);
			
			for(Entry<Integer, SimpleVertex> entry : cv.getVSet().entrySet()) {
				//System.out.println("@ "+entry.getValue().toString());
				Data data = cluster.getData(entry.getValue().getId());
				data.setData_virtual_data_id(cv.getId());
				data.setData_inUse(true);
			}
		}
		
		// Creating Compressed Hyper-graph Workload File
		Global.LOGGER.info("Total "+vedge.size()+" virtual hyperedges containing "+vvertex.size()
				+" virtual tuples have been identified for repartitioning.");
		Global.LOGGER.info("Generating workload file for compressed hypergraph based repartitioning ...");
			
		int edges = vedge.size();
		int vertices = vvertex.size();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;
				
		// Write in a file		
		if(edges <= 1) { 
			Global.LOGGER.info("Only "+edges+" compressed hyperedges present in the workload hypergraph network.");
			Global.LOGGER.info("Repartitioning will be aborted for this run ...");			
			return true;
			
		}else{		
			try {
				wb.getWrl_file().getParentFile().mkdirs();
				
				if(wb.getWrl_file().exists())
					wb.getWrl_file().delete();
				
				wb.getWrl_file().createNewFile();
				
				Writer writer = null;
				
				try {
					writer = new BufferedWriter(new OutputStreamWriter(
							new FileOutputStream(wb.getWrl_file()), "utf-8"));
					writer.write(edges+" "+vertices+" "+hasTransactionWeight+""+hasDataWeight+"\n");
					
					for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : vedge.entrySet()) {
						
						// Writing e' weight						
						writer.write(Integer.toString(entry.getKey().getWeight())+" ");
								
						// Writing v' incident on e'
						Iterator<CompressedVertex> cv_itr =  entry.getValue().iterator();
						while(cv_itr.hasNext()) {						
							writer.write(Integer.toString(vvertex_id_map.get(cv_itr.next().getId())));
							
							if(cv_itr.hasNext())
								writer.write(" "); 
						}
						
						writer.write("\n");		
					}
	
					// Writing v' weight					
					Iterator<CompressedVertex> cv_itr = vvertex.iterator();
					while(cv_itr.hasNext()) {
						String cv_weight = Integer.toString(cv_itr.next().getWeight());
						writer.write(cv_weight);
						
						if(cv_itr.hasNext())
							writer.write("\n");
					}
					
				} catch(IOException e) {
					e.printStackTrace();
				}finally {
					writer.close();
				}
			} catch (IOException e) {		
				e.printStackTrace();
			}	
			
			Global.LOGGER.info("Workload file generation for compressed hypergraph based repartitioning has completed.");
			return false;
		}
	}
		
	// Generates Workload File for Graph partitioning
	private static boolean generateGraphWorkloadFile(Cluster cluster, WorkloadBatch wb) throws IOException {

		vertex_id_map = new HashMap<Integer, Integer>();
		
		int vertex_id = 0;
		
		for(SimpleVertex v : wb.hgr.getVertices()) {
			
			vertex_id_map.put(v.getId(), ++vertex_id);
				
			Data data = cluster.getData(v.getId());			
			data.setData_shadowId(vertex_id);
			data.setData_inUse(true);			
		}		
		
		int edges = 0;		
		int vertices = wb.hgr.getVertexCount();
		int hasTransactionWeight = 1;
		int hasDataWeight = 1;
		
		String content = "";		
		for(SimpleVertex v : wb.hgr.getVertices()) {

			String str = Integer.toString(v.getWeight())+" ";
			
			for(SimpleVertex _v : wb.hgr.getNeighbors(v)) {
				if(!v.equals(_v)) {
					SimpleHEdge _h = wb.hgr.findEdge(v, _v);
					
					++edges;
										
					String _id = Integer.toString(vertex_id_map.get(_v.getId()));
					String _edge_weight = Integer.toString(_h.getWeight());
					
					str += _id+" "+_edge_weight+" ";
				}
			}

			str += "\n";
			content += str;
		}
					
		// Writing in a file
		if(edges <= 1 ) {			
			Global.LOGGER.info("Only "+edges+" edges present in the workload graph network.");
			Global.LOGGER.info("Repartitioning will be aborted for this run ...");			
			return true;
			
		} else {		
			try {
				wb.getWrl_file().getParentFile().mkdirs();
				
				if(wb.getWrl_file().exists())
					wb.getWrl_file().delete();
				
				wb.getWrl_file().createNewFile();
				
				Writer writer = null;
				
				try {
					writer = new BufferedWriter(new OutputStreamWriter(
							new FileOutputStream(wb.getWrl_file()), "utf-8"));
					writer.write(vertices+" "+(edges/2)+" "+hasDataWeight+""+hasTransactionWeight
							+"\n"+content);
					
				} catch(IOException e) {
					e.printStackTrace();
				}finally {
					writer.close();
				}
			} catch (IOException e) {		
				e.printStackTrace();
			}
			
			Global.LOGGER.info("Workload file generation for graph based repartitioning has completed.");
			Global.LOGGER.info("Total edges: "+edges);
			Global.LOGGER.info("Total vertices: "+vertices);
			
			return false;
		}		
	}	
	
	public static void processPartFile(Cluster cluster, WorkloadBatch wb, int partition_numbers) 
			throws IOException {
		
		Map<Integer, Integer> keyMap = new HashMap<Integer, Integer>();		
		String wrl_file_name = null;
		String part_file_name = null;
		
		wrl_file_name = Global.repartitioningCycle+"-"+Global.simulation;		
		part_file_name = wrl_file_name+".part."+partition_numbers;	
			
		File part_file = new File(Global.part_dir+Global.getRunDir()+part_file_name);
		
		int key = 1;		
		//System.out.println("@ - "+part_file_name);
		Scanner scanner = new Scanner(part_file);
		try {
			while(scanner.hasNextLine()) {
				int cluster_id = Integer.valueOf(scanner.nextLine());								
				keyMap.put(key, cluster_id);	
				//System.out.println("@debug >> key: "+key+" | Cluster: "+cluster_id);				
				++key;
			}						
		} finally {
			scanner.close();
		}					
		
		if(Global.associative)
			WorkloadBatchProcessor.processClusterElements(cluster, wb, Global.dsm.hgr, keyMap);
		else
			WorkloadBatchProcessor.processClusterElements(cluster, wb, wb.hgr, keyMap);
	}
	
	private static void processClusterElements(Cluster cluster, WorkloadBatch wb, 
			ISimpleHypergraph<SimpleVertex, SimpleHEdge> hgr, Map<Integer, Integer> keyMap) {
		
		Set<Integer> dataSet = new HashSet<Integer>();
		
		for(SimpleVertex v : hgr.getVertices()) {
			Data data = cluster.getData(v.getId());
			
			if(!dataSet.contains(v.getId()) && data.isData_inUse()) {
				dataSet.add(v.getId());
				
				int shadow_id = -1;
				int cluster_id = -1;
				int virtual_id = -1;
				
				switch(Global.workloadRepresentation) {
				
					case "hgr":
						if(Global.compressionEnabled) {
							//System.out.println(">> x="+"|keyMap.get(x)="+keyMap.get(x));									
							//virtual_id = data.getData_virtual_data_id();
							virtual_id = vvertex_id_map.get(data.getData_virtual_data_id());
							cluster_id = keyMap.get(virtual_id) + 1;
							
							data.setData_chmetisClusterId(cluster_id);
							
							wb.getWrl_virtualDataId_clusterId_map().put(data.getData_virtual_data_id(), cluster_id);
							wb.getWrl_dataId_clusterId_map().put(data.getData_id(), cluster_id);
							
						} else {							
							shadow_id = vertex_id_map.get(data.getData_id());
							cluster_id = keyMap.get(shadow_id) + 1;
							
							data.setData_hmetisClusterId(cluster_id);
							
							wb.getWrl_dataId_clusterId_map().put(data.getData_id(), cluster_id);
						}
						
						break;
						
					case "gr":
						shadow_id = vertex_id_map.get(data.getData_id());
						cluster_id = keyMap.get(shadow_id) + 1;
						
						data.setData_metisClusterId(cluster_id);
						
						wb.getWrl_dataId_clusterId_map().put(data.getData_id(), cluster_id);
						
						break;
				}						

				//System.out.println("@debug >> "+data.toString()+" | S="+shadow_id+" | C="+cluster_id+" | V="+virtual_id);
				
				data.setData_shadowId(-1);
				data.setData_hasShadowId(false);					
			}
		} // end -- for()-Data		
	}
}