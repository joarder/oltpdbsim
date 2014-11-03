/**
 * @author Joarder Kamal
 */

package main.java.repartition;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.incmine.core.SemiFCI;
import main.java.incmine.learners.IncMine;
import main.java.incmine.streams.ZakiFileStream;
import main.java.utils.Utility;
import main.java.utils.graph.SimGraph;
import main.java.utils.graph.SimHypergraph;
import main.java.utils.graph.SimpleEdge;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class TransactionClassifier {	

	private static IncMine learner;
	private static ZakiFileStream stream;
	
	public static void init() {
		
		learner = new IncMine();
		
		// Configure the learner
		learner.minSupportOption.setValue(0.05d);
		learner.relaxationRateOption.setValue(0.5d);
		learner.fixedSegmentLengthOption.setValue(1000); //1000		
		learner.windowSizeOption.setValue(10);
		learner.maxItemsetLengthOption.setValue(-1);
		learner.resetLearning();
	}
	
	private static void mine(Cluster cluster, WorkloadBatch wb){
		
		String file = Integer.toString(wb.getWrl_id())+"-"+Global.simulation;
		
		// Generates the DSM file
		wb.setMiner_prWriter(Utility.getPrintWriter(Global.mining_dir, file));
		wb.prepareMiningFile(cluster);		

		// Read the stream input
		stream = new ZakiFileStream(Global.mining_dir+file+".txt");
		stream.prepareForUse();				
        
		// Perform DSM
		while(stream.hasMoreInstances()){
        	learner.trainOnInstance(stream.nextInstance());            
        }		
	}
	
	// DSM (FD)
//	public static int classifyMovableFD(Cluster cluster, WorkloadBatch wb) {		
//		
//		init();
//		
//		//Find the list of semi-FCI
//		mine(cluster, wb);		
//		//System.out.println(this.learner);
//	
//		//Find the list of distributed semi-FCI
//		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();
//		ArrayList<List<Integer>> distributedSemiFCIList = new ArrayList<List<Integer>>();
//		
//		for(SemiFCI semiFCI : learner.getFCITable()){
//			//System.out.println("@ "+semiFCI.getItems());
//			
//			if(semiFCI.getItems().size() > 1){
//				//System.out.println("@ "+semiFCI.getItems());
//				semiFCIList.add(semiFCI.getItems());
//				
//				if(isDistributedFCI(cluster, semiFCI.getItems()))
//					distributedSemiFCIList.add(semiFCI.getItems());
//			}
//        }
//		
//		Global.LOGGER.info("Total "+distributedSemiFCIList.size()+" distributed semi-frequent closed data tuple sets have been identified.");
//		
//		//Find the transactions containing distributed semi-FCI
//		int orange_data = 0, green_data = 0;
//		int tr_red = 0, tr_orange = 0, tr_green = 0;
//		int tr_id = 0;
//		Set<Integer> frequent_dsfci = new TreeSet<Integer>();
//		
//		for(Entry<Integer, Map<Integer, Transaction>> entry : wb.getTrMap().entrySet()) {
//			Set<Integer> toBeRemoved = new TreeSet<Integer>();
//			
//			for(Entry<Integer, Transaction> tr_entry : entry.getValue().entrySet()) {
//				Transaction transaction = tr_entry.getValue();
//				
//				orange_data = 0;
//				green_data = 0;
//				
//				tr_id = transaction.getTr_id();
//				
//				// Infrequent Transactions
//				if(!isFrequent(transaction, semiFCIList)){
//					if(!toBeRemoved.contains(tr_id))
//						toBeRemoved.add(tr_id);						
//				}//end-if
//				// Frequent Transactions
//				else{
//					// Distributed Transactions
//					if(transaction.getTr_serverSpanCost() > 0){
//						// Distributed Transactions containing Distributed Semi-FCI
//						if(containsDistributedSemiFCI(transaction, distributedSemiFCIList)){
//							++tr_red;
//							transaction.setTr_class("red");
//							frequent_dsfci.add(transaction.getTr_id());
//						}//end-if
//						// Distributed Transactions containing Non-Distributed Semi-FCI
//						else{
//							if(!toBeRemoved.contains(tr_id))
//								toBeRemoved.add(tr_id);
//						}//end-else
//					}//end-if
//					// Non-Distributed Transactions
//					else{
//						// Evaluating Movable and Non-Movable Transactions
//						Iterator<Integer> data_iterator = transaction.getTr_dataSet().iterator();
//						while(data_iterator.hasNext()) {
//							Data data = cluster.getData(data_iterator.next());
//							
//							int tr_counts = wb.getWrl_dataTransactionsInvolved().get(data.getData_id()).size();
//							
//							if(tr_counts <= 1) 
//								++green_data;
//							else {							
//								for(int tid : wb.getWrl_dataTransactionsInvolved().get(data.getData_id())) {
//									Transaction tr = wb.getTransaction(tid);
//									if(tr.getTr_serverSpanCost() > 0){
//										if(frequent_dsfci.contains(tr.getTr_id()))
//											++orange_data;
//										else{
//											if(!toBeRemoved.contains(transaction.getTr_id()))
//												toBeRemoved.add(transaction.getTr_id());
//										}//end-else												
//									}//end-if
//								}//end-for() 
//							}//end-else
//						}//end-while()
//						
//						if(transaction.getTr_dataSet().size() == green_data) { 
//							transaction.setTr_class("green");
//							++tr_green;
//							
//							if(!toBeRemoved.contains(transaction.getTr_id()))
//								toBeRemoved.add(transaction.getTr_id());
//						}
//						
//						if(orange_data > 0) {
//							transaction.setTr_class("orange");
//							++tr_orange;
//						}
//					}//end-else
//				}//end-else
//			}//end-for()
//			
//			// Removing the selected Transactions from the Workload
//			if(toBeRemoved.size() > 0)
//				wb.removeTransactions(toBeRemoved, entry.getKey());					
//		}//end-for()
//		
//		Global.LOGGER.info("Classified "+tr_red+" transactions as RED !!!");
//		
//		wb.setWrl_tr_green(tr_green);
//		Global.LOGGER.info("Classified "+tr_green+" transactions as GREEN !!!");
//		
//		wb.setWrl_tr_orange(tr_orange);		
//		Global.LOGGER.info("Classified "+tr_orange+" transactions as ORANGE !!!");
//		
//		return (tr_red + tr_orange);
//	}
//	
//	// DSM - (FD+FND)
//	public static int classifyMovableFDFND(Cluster cluster, WorkloadBatch wb) {
//		
//		init();
//		
//		//Find the list of semi-FCI
//		mine(cluster, wb);		
//		//System.out.println(this.learner);
//	
//		//Find the list of semi-FCI
//		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();		
//		for(SemiFCI semiFCI : learner.getFCITable()){
//			//System.out.println("@ "+fci.getItems());
//			
//			if(semiFCI.getItems().size() > 1){
//				//System.out.println("@ "+fci.getItems());
//				semiFCIList.add(semiFCI.getItems());				
//			}
//        }
//		
//		Global.LOGGER.info("Total "+semiFCIList.size()+" semi-frequent closed data tuple sets have been identified.");
//		
//		//Find the transactions containing semi-FCI
//		int orange_data = 0, green_data = 0;
//		int tr_red = 0, tr_orange = 0, tr_green = 0;
//		int tr_id = 0;
//		Set<Integer> frequent_sfci = new TreeSet<Integer>();
//		
//		for(Entry<Integer, Map<Integer, Transaction>> entry : wb.getTrMap().entrySet()) {
//			Set<Integer> toBeRemoved = new TreeSet<Integer>();
//			
//			for(Entry<Integer, Transaction> tr_entry : entry.getValue().entrySet()) {				
//				Transaction transaction = tr_entry.getValue();
//						
//					orange_data = 0;
//					green_data = 0;
//				
//					tr_id = transaction.getTr_id();
//				
//					// Infrequent Transactions
//					if(!isFrequent(transaction, semiFCIList)){
//						if(!toBeRemoved.contains(tr_id))
//							toBeRemoved.add(tr_id);						
//					}//end-if
//					// Frequent Transactions
//					else{
//						// Distributed Transactions
//						if(transaction.getTr_serverSpanCost() > 0){
//							++tr_red;
//							transaction.setTr_class("red");
//							frequent_sfci.add(transaction.getTr_id());
//							
//						}//end-if
//						// Non-Distributed Transactions
//						else{
//							// Evaluating Movable and Non-Movable Transactions
//							Iterator<Integer> data_iterator = transaction.getTr_dataSet().iterator();
//						
//							while(data_iterator.hasNext()) {
//								Data data = cluster.getData(data_iterator.next());
//							
//								int tr_counts = wb.getWrl_dataTransactionsInvolved().get(data.getData_id()).size();
//							
//								if(tr_counts <= 1) 
//									++green_data;
//								else {							
//								
//									for(int tid : wb.getWrl_dataTransactionsInvolved().get(data.getData_id())) {
//										Transaction tr = wb.getTransaction(tid);
//									
//										if(tr.getTr_serverSpanCost() > 0){
//											if(frequent_sfci.contains(tr.getTr_id()))
//												++orange_data;
//											else{
//												if(!toBeRemoved.contains(transaction.getTr_id()))
//													toBeRemoved.add(transaction.getTr_id());
//											}//end-else												
//										}//end-if
//									}//end-for() 
//								}//end-else
//							}//end-while()
//						
//							if(transaction.getTr_dataSet().size() == green_data) { 
//								transaction.setTr_class("green");
//								++tr_green;
//							
//								if(!toBeRemoved.contains(transaction.getTr_id()))
//									toBeRemoved.add(transaction.getTr_id());
//							}
//						
//							if(orange_data > 0) {
//								transaction.setTr_class("orange");
//								++tr_orange;
//							}
//						}//end-else
//					}//end-else			
//			}//end-for()
//		
//			// Removing the selected Transactions from the Workload
//			if(toBeRemoved.size() > 0)
//				wb.removeTransactions(toBeRemoved, entry.getKey());
//			
//		}//end-for()
//		
//		Global.LOGGER.info("Classified "+tr_red+" transactions as RED !!!");
//		
//		wb.setWrl_tr_green(tr_green);
//		Global.LOGGER.info("Classified "+tr_green+" transactions as GREEN !!!");
//		
//		wb.setWrl_tr_orange(tr_orange);		
//		Global.LOGGER.info("Classified "+tr_orange+" transactions as ORANGE !!!");
//		
//		return (tr_red + tr_orange);
//	}		
	
	// Returns true if a transaction contains any of the mined semi-FCI
	private static boolean isFrequent(Transaction transaction, ArrayList<List<Integer>> semiFCIList){
		for(List<Integer> semiFCI : semiFCIList){
			if(transaction.getTr_dataSet().containsAll(semiFCI))
				return true;
		}
		
		return false;
	}
	
	// Returns true if a transaction contains any of the mined distributed semi-FCI
	private static boolean containsDistributedSemiFCI(Transaction transaction, ArrayList<List<Integer>> distributedSemiFCIList){
		for(List<Integer> dSemiFCI : distributedSemiFCIList){
			if(transaction.getTr_dataSet().containsAll(dSemiFCI))
				return true;
		} 
		
		return false;
	}	
	
	// Returns true if a FCI is distributed between two or more physical servers
	private static boolean isDistributedFCI(Cluster cluster, List<Integer> semiFCI){
		Data data;
		Set<Integer> nidSet = new TreeSet<Integer>();
		
		for(Integer t : semiFCI){
			data = cluster.getData(t);
			
			nidSet.add(data.getData_server_id());
			if(nidSet.size() > 1)
				return true;
		}
				
		return false;
	}
	
	static int tr_red = 0;
	static int tr_orange = 0;
	static int tr_green = 0;
	static Set<Integer> movable = new TreeSet<Integer>();
	static Set<Integer> toBeRemoved = new TreeSet<Integer>();
	
	// Basic classification
	public static int classifyMovableDTs(Cluster dbCluster, WorkloadBatch wb) {
		
		//Find the distributed movable transactions
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			Transaction tr = wb.getTransaction(h.getId());
			
			if(tr.isDt()) {
				tr.setTr_class("red");
				++tr_red;
				
			} else {
				
				if(isMovable(wb, tr)) { 
					tr.setTr_class("orange");
					++tr_orange;
					
					movable.add(tr.getTr_id());
					
				} else {				
					tr.setTr_class("green");
					++tr_green;
					
					toBeRemoved.add(tr.getTr_id());
				}
			}
		}
		
		for(Integer i : toBeRemoved) {
			
			switch(Global.workloadRepresentation) {
				case "gr":
					Set<Integer> edgeSet = WorkloadBatch.edgeIdMap.get(i);
					for(Integer e_i : edgeSet) {
						SimpleEdge e = wb.gr.getEdge(e_i);
						wb.gr.removeEdge(e);
					}
					
					WorkloadBatch.edgeIdMap.remove(i);
					
					break;
					
				case "hgr":
					SimpleHEdge h = wb.hgr.getHEdge(i);
					wb.hgr.removeEdge(h);
					
					break;
			}
		}
						
		Global.LOGGER.info("Classified "+tr_red+" transactions as purely distributed !!!");		
		Global.LOGGER.info("Classified "+tr_green+" transactions as non distributed !!!");			
		Global.LOGGER.info("Classified "+tr_orange+" transactions as non distributed but movable !!!");		
		
		wb.setWrl_tr_red(tr_red);
		wb.setWrl_tr_green(tr_green);
		wb.setWrl_tr_orange(tr_orange);
		
		tr_red = 0;
		tr_orange = 0;
		tr_green = 0;
		
		return (tr_red + tr_orange);
	}
		
	private static boolean isMovable(WorkloadBatch wb, Transaction tr) {
		
		for(Integer d : tr.getTr_dataSet()) {			
			SimpleVertex v = wb.hgr.getVertex(d);
			
			for(SimpleHEdge nh : wb.hgr.getIncidentEdges(v)) {				
				Transaction incident_tr = wb.getTransaction(nh.getId());
				
				if(incident_tr.isDt() && !incident_tr.equals(tr))					
					return true;
			}
		}
		
		return false;
	}
}