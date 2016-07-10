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

package main.java.dsm;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Queue;

import main.java.cluster.Cluster;
import main.java.entry.Global;
import main.java.incmine.core.SemiFCI;
import main.java.incmine.learners.IncMine;
import main.java.incmine.streams.ZakiFileStream;
import main.java.repartition.DataMigration;
import main.java.repartition.WorkloadBatchProcessor;
import main.java.utils.Utility;
import main.java.utils.graph.ISimpleHypergraph;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleHypergraph;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;
import main.java.workload.WorkloadExecutor;

import com.google.common.collect.EvictingQueue;

public class DataStreamMining {

	public ISimpleHypergraph<SimpleVertex, SimpleHEdge> hgr;
	public static HashMap<Integer, FCICluster> fci_clusters;
	Queue<String> dsm_queue;
	public IncMine dsm_learner;
	private ZakiFileStream dsm_stream;
	private PrintWriter dsm_PrintWriter;	
	private String dsm_dumpfilename;
	private File dsm_dumpfile;
	private int dsm_dump_serial;
	private int dsm_line_serial;
	private static int dsm_size;
	
	public DataStreamMining() {
		dsm_size = Global.observationWindow * Global.streamCollectorSizeFactor;
		
		this.dsm_dump_serial = 0;
		this.dsm_line_serial = 0;
		this.dsm_queue = EvictingQueue.create(dsm_size); // Circular FIFO
		this.dsm_learner = new IncMine();
		
		// Configure the learner
		this.dsm_learner.minSupportOption.setValue(0.1d); // 0.1 as the default value
		this.dsm_learner.relaxationRateOption.setValue(0.5d); // 0.5 as the default value
		this.dsm_learner.fixedSegmentLengthOption.setValue(Global.observationWindow); // 1000 is the default value 		
		this.dsm_learner.windowSizeOption.setValue(Global.streamCollectorSizeFactor); // 10 is the default value
		this.dsm_learner.maxItemsetLengthOption.setValue(-1); // - 1 means disabled, will perform full stream mining 
		this.dsm_learner.resetLearning();
	}
	
	public void collectStream(Cluster cluster, Transaction tr) {
		boolean isDumpFull = false;
		String stream = "";
		
		++this.dsm_line_serial;
		
		Iterator<Integer> data = tr.getTr_dataSet().iterator();			
		stream += this.dsm_line_serial+" "+this.dsm_line_serial+" "+tr.getTr_dataSet().size()+" ";
		
		while(data.hasNext()) {
			stream += cluster.getData(data.next()).getData_id();
			
			if(data.hasNext())
				stream += " ";
		}
		
		// Add the current stream in the queue
		this.dsm_queue.add(stream);
		
		if(this.dsm_line_serial == dsm_size) 
			isDumpFull = true;
		
		if(isDumpFull) {
			isDumpFull = false;
			this.dsm_line_serial = 0;
			
			// Get the latest streams in a file
			this.prepareDSMDumpFile();
		}
	}		
	
	// Prepares a dump file containing most recent transaction streams based on the queue size
	private void prepareDSMDumpFile() {
		++this.dsm_dump_serial;
		
		this.dsm_dumpfilename = Global.mining_dir+"run"+Global.repeatedRuns+"/"
									+Global.simulation+"-dump-"+this.dsm_dump_serial+".txt";
		
		this.dsm_dumpfile = new File(this.dsm_dumpfilename);
		this.dsm_PrintWriter = Utility.getPrintWriter(Global.mining_dir, this.dsm_dumpfile);
		
		// Write in the dump file
		Iterator<String> itr = this.dsm_queue.iterator();
		while(itr.hasNext()) {
			this.dsm_PrintWriter.println(itr.next());
		}
		
		this.dsm_PrintWriter.flush();
		this.dsm_PrintWriter.close();
	}
	
	public void performDSM(Cluster cluster, WorkloadBatch wb, long start_time) {		
		// Read the stream input
		this.dsm_stream = new ZakiFileStream(this.dsm_dumpfilename);
		this.dsm_stream.prepareForUse();				
        
		// Perform DSM
		while(dsm_stream.hasMoreInstances()){
			this.dsm_learner.trainOnInstance(this.dsm_stream.nextInstance());            
        }
		
		// Testing
		//System.out.println(this.dsm_learner);		
		Global.LOGGER.info("Total "+this.dsm_learner.getFCITable().size()+" frequent tuple sets have been identified.");
		
		if(this.dsm_learner.getFCITable().size() == 0) {
			Global.isFrequentClustersFound = false;
			Global.LOGGER.info("No frequent tuple sets have been identified !!! Nothing to do at this moment.");
		} else 
			Global.isFrequentClustersFound = true;
		
		if(Global.associative && Global.isFrequentClustersFound)
			this.performARHC(cluster, wb, start_time);
	} 
	
	// Association Rule Hypergraph Clustering (ARHC) for both adaptive and non-adaptive algorithms
	private void performARHC(Cluster cluster, WorkloadBatch wb, long start_time) {
		// Create a hypergraph from the FCI list
		Global.LOGGER.info("Creating association rule hypergraph ...");
		
		hgr = new SimpleHypergraph<SimpleVertex, SimpleHEdge>();
		fci_clusters = new HashMap<Integer, FCICluster>();
		
		ArrayList<FCIHEdge> fciHEdgeList = new ArrayList<FCIHEdge>();
		HashMap<Integer, Integer> vertexMap = new HashMap<Integer, Integer>();
		int hEdgeId = 0;
		
		for(SemiFCI semiFCI : this.dsm_learner.getFCITable()){
			//System.out.println("\t-- "+semiFCI.getItems());
			//System.out.println("\t-- Current Support = "+semiFCI.currentSupport());
			//System.out.println("\t-- Approximate Support = "+semiFCI.getApproximateSupport());
			
			int fci_support = semiFCI.getApproximateSupport();
			int fci_weight = 0;
			
			if(fci_support < 1) {
				fci_support = 1;
				fci_weight = 1;
			} else
				fci_weight = (int)(fci_support/Global.streamCollectorSizeFactor);
			
			HashSet<Integer> vertexSet = new HashSet<Integer>();
			
			if(semiFCI.getItems().size() > 1){					
				double hEdgeWeight = 0.0;
				
				for(int fci : semiFCI.getItems()) {					
					vertexSet.add(fci);
					vertexMap.put(fci, fci_weight);
					
					hEdgeWeight += fci_support;
				}
		
				fciHEdgeList.add(new FCIHEdge(
						++hEdgeId, (int)hEdgeWeight/semiFCI.getItems().size(), vertexSet));				
				
			} else {
				vertexMap.put(semiFCI.getItems().get(0), fci_weight);
			}
        } // end-for()
		
		// Creating actual hypergraph
		for(FCIHEdge h : fciHEdgeList) {
			// Updating vertex weight
			for(int v : h.vSet)
				h.vMap.put(v, vertexMap.get(v));
				
			hgr.addHEdge(new SimpleHEdge(h.id, h.weight), wb.getVertices(cluster, h.vMap));
		} // end-for()
		
		Global.LOGGER.info("An association rule hypergraph is created containing "+hgr.getEdgeCount()+" hyperedges containing "
				+hgr.getVertexCount()+" vertices for clustering.");
		
		// Workload file
		String wrl_file_name = Global.repartitioningCycle+"-"+Global.simulation; 
		String wrl_abs_file_name = Global.part_dir+Global.getRunDir()+wrl_file_name;						
		
		File workloadFile = new File(wrl_abs_file_name);
		
		wb.setWrl_file_name(wrl_abs_file_name);
		wb.setWrl_file(workloadFile);
		
		// Performing ARHP and data migration
		Global.LOGGER.info("Redistributing the frequently occurred tuplesets after ARHC ...");
		WorkloadBatchProcessor.generateHGraphWorkloadFile(cluster, wb, hgr);
		WorkloadExecutor.runRepartitioner(cluster, wb);		
		DataMigration.performDataMigration(cluster, wb);		
				
		// Testing
		/*for(SimpleVertex v : this.hgr.getVertices()) {
			Data data = cluster.getData(v.getId());
			System.out.println(">> V"+v.getId()+"|W="+v.getWeight()+"|S"+v.getSid()+"|P"+v.getPid()+"|Incident h="+wb.hgr.getIncidentEdges(v).size());
			System.out.println(data.toString());
		}*/
		
		// Preparing the clusters with their associated weights
		Global.LOGGER.info("Preparing the ARHC clusters with their associated weights ...");
		double maxClusterWeight = Double.MIN_VALUE;
		for(Entry<Integer, FCICluster> fci_cluster : fci_clusters.entrySet()) {
			// Get the FCI weight
			double clusterWeight = 0.0;
			for(int fci : fci_cluster.getValue().fci)
				clusterWeight += vertexMap.get(fci);			
			
			if(clusterWeight >= maxClusterWeight)
				maxClusterWeight = clusterWeight;
			
			fci_cluster.getValue().weight = clusterWeight;			
		}
		
		// Normalizing cluster Weights
		Global.LOGGER.info("Normalizing the ARHC clusters' weights ...");
		for(Entry<Integer, FCICluster> fci_cluster : fci_clusters.entrySet()) {
			fci_cluster.getValue().weight = fci_cluster.getValue().weight/maxClusterWeight;
			Global.LOGGER.info(""+fci_cluster);
		}
				
		// Execution - Data migration -- this is where ARHC and A-ARHC MD repartitioning separate
		if(!Global.adaptive) {			
			Global.LOGGER.info("Start processing all DTs within the current workload observation window ...");
			DataMigration.strategyARHC(cluster, wb, start_time);
			Global.LOGGER.info("Done with transaction processing and required data migrations!!");
		}			
	}
}

//
class FCIHEdge{
	int id;
	int weight;
	HashSet<Integer> vSet;
	HashMap<Integer, Integer> vMap;
	
	public FCIHEdge(int id, int weight, HashSet<Integer> vertexSet) {
		this.id = id;
		this.weight = weight;
		this.vSet = new HashSet<Integer>(vertexSet);
		this.vMap = new HashMap<Integer, Integer>();
	}
}