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
import java.util.TreeSet;

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
				if(Global.compressionEnabled)
					empty = generateCGraphWorkloadFile(cluster, wb);
				else
					empty = generateGraphWorkloadFile(cluster, wb);
				
				break;
		}
		
		return empty;
	}
	
	private static Map<Integer, Integer> vertex_id_map;
	private static Map<Integer, Integer> cvertex_id_map;
	
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
		int hasHEdgeWeight = 1;
		//int hasVertexWeight = 0;
		
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
					writer.write(edges+" "+vertices+" "+hasHEdgeWeight+"\n");//+""+hasVertexWeight+"\n");
					
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
					/*Iterator<SimpleVertex> v_itr = hgr.getVertices().iterator();
					while(v_itr.hasNext()) {
						String v_weight = Integer.toString(v_itr.next().getWeight());							
						writer.write(v_weight);
						
						if(v_itr.hasNext())
							writer.write("\n"); 
					}*/							
					
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
		
		// Compressing workload hypergraph
		Global.LOGGER.info("Compressing current workload hypergrah ...");
		
		// Compressed hypergraph initialization
		wb.hgr.setCHEdges(new HashMap<CompressedHEdge, Set<CompressedVertex>>());
		wb.hgr.setCVertices(new HashMap<CompressedVertex, Set<CompressedHEdge>>());				
		wb.hgr.setCHEMap(new HashMap<Integer, CompressedHEdge>());
		wb.hgr.setCVMap(new HashMap<Integer, CompressedVertex>());
		
		// Compressing hypergraph vertices
		/*for(SimpleVertex v : wb.hgr.getVertices())
			wb.hgr.addCVertex(v);*/		
				
		//Global.LOGGER.info(wb.hgr.getCVertices().size()+" compressed vertices are retrieved from "+wb.hgr.getVertexCount()+" hypergraph vertices.");
		
		// Compressing hyperedges
		Global.cHEdgeSeq = 0;
		for(SimpleHEdge h : wb.hgr.getEdges())
			wb.hgr.addCHEdge(h);
		
		Global.LOGGER.info(wb.hgr.getCHEdges().size()+" compressed hyperedges "
				+ "containing "+wb.hgr.getCVertices().size()+" compressed vertices"
				+ "are created from "+wb.hgr.getEdgeCount()+" hyperedges.");

		// Only selecting compressed hyperedges having at least two compressed vertices
		Global.LOGGER.info("Only selecting the compressed hyperedges having at least two compressed vertices ...");
		
		Map<CompressedHEdge, Set<CompressedVertex>> cHESet = new HashMap<CompressedHEdge, Set<CompressedVertex>>();
		Set<CompressedVertex> cVSet = new TreeSet<CompressedVertex>();
						
		for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : wb.hgr.getCHEdges().entrySet()) {
			if(entry.getValue().size() >= 2) {
				cHESet.put(entry.getKey(), entry.getValue());
				cVSet.addAll(entry.getValue());
			} else {				
				Global.LOGGER.info("Omitting compressed hyperedge with only 1 compressed vertex !!!");
			}
		}				
		
		Global.LOGGER.info("Total "+cHESet.size()+" compressed hyperedges spanning "+cVSet.size()+" compressed vertices are selected.");
		
		cvertex_id_map = new HashMap<Integer, Integer>();
		int temp_cv_id = 0;
		for(CompressedVertex cv : cVSet){			
			cvertex_id_map.put(cv.getId(), ++temp_cv_id);
			
			for(Entry<Integer, SimpleVertex> cv_entry : cv.getVSet().entrySet()) {							
				Data data = cluster.getData(cv_entry.getValue().getId());
				data.setData_inUse(true);
			}
		}	
		
		// Creating Compressed Hyper-graph Workload File
		Global.LOGGER.info("Generating workload file for compressed hypergraph based repartitioning ...");
			
		int edges = cHESet.size();
		int vertices = cVSet.size();
		int hasCHEdgeWeight = 1;
		//int hasCVertexWeight = 0;
				
		// Write in a file		
		if(edges <= 1) { 
			Global.LOGGER.info("Only "+edges+" compressed hyperedges present in the workload hypergraph network.");
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
					writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(wb.getWrl_file()), "utf-8"));
					writer.write(edges+" "+vertices+" "+hasCHEdgeWeight+"\n");//+""+hasCVertexWeight+"\n");
					
					for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : cHESet.entrySet()) {						
						// Writing ch's weight
						writer.write(Integer.toString(entry.getKey().getWeight())+" ");
								
						// Writing compressed vertices incident on ch
						Iterator<CompressedVertex> cv_itr =  entry.getValue().iterator();
						while(cv_itr.hasNext()) {						
							writer.write(Integer.toString(cvertex_id_map.get(cv_itr.next().getId())));
							
							if(cv_itr.hasNext())
								writer.write(" "); 
						}
						
						writer.write("\n");		
					}
	
					// Writing compressed vertices' weight					
					/*Iterator<CompressedVertex> cv_itr = cVSet.iterator();
					while(cv_itr.hasNext()) {
						String cv_weight = Integer.toString(cv_itr.next().getWeight());
						writer.write(cv_weight);
						
						if(cv_itr.hasNext())
							writer.write("\n");
					}*/
					
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
		int hasEdgeWeight = 1;
		int hasVertexWeight = 0;
		
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
					writer.write(vertices+" "+(edges/2)+" 0"+hasVertexWeight+""+hasEdgeWeight
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
	
	// Generates Workload File for Graph partitioning
	private static boolean generateCGraphWorkloadFile(Cluster cluster, WorkloadBatch wb) throws IOException {

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
		int hasCEdgeWeight = 1;
		int hasCVertexWeight = 0;
		
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
					writer.write(vertices+" "+(edges/2)+" 0"+hasCVertexWeight+""+hasCEdgeWeight
							+"\n"+content);
					
				} catch(IOException e) {
					e.printStackTrace();
				}finally {
					writer.close();
				}
			} catch (IOException e) {		
				e.printStackTrace();
			}
			
			Global.LOGGER.info("Workload file generation for compressed graph based repartitioning has completed.");
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
				int temp_compressed_cv_id = -1;
				
				switch(Global.workloadRepresentation) {
				//System.out.println("@debug >> x="+"|keyMap.get(x)="+keyMap.get(x));
					case "hgr":
						if(Global.compressionEnabled) {							
							temp_compressed_cv_id = cvertex_id_map.get(data.getData_compressed_data_id());
							cluster_id = keyMap.get(temp_compressed_cv_id) + 1;							
							data.setData_chmetisClusterId(cluster_id);
							
						} else {							
							shadow_id = vertex_id_map.get(data.getData_id());
							cluster_id = keyMap.get(shadow_id) + 1;							
							data.setData_hmetisClusterId(cluster_id);
						}
						
						break;
						
					case "gr":
						shadow_id = vertex_id_map.get(data.getData_id());
						cluster_id = keyMap.get(shadow_id) + 1;						
						data.setData_metisClusterId(cluster_id);						
						break;
				}						

				//System.out.println("@debug >> "+data.toString()+" | S="+shadow_id+" | C="+cluster_id+" | V="+compressed_id);
				
				data.setData_shadowId(-1);
				data.setData_hasShadowId(false);					
			}
		} // end -- for()-Data		
	}
}