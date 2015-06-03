/**
 * @author Joarder Kamal
 */

package main.java.repartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.incmine.core.SemiFCI;
import main.java.incmine.learners.IncMine;
import main.java.incmine.streams.ZakiFileStream;
import main.java.utils.Utility;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class TransactionClassifier {	

	private static IncMine incMine_learner;
	private static ZakiFileStream stream;

	private static int DT = 0;
	private static int nonDT = 0;
	private static int movableNonDt = 0;		
	
	private static int DtContainingFreqDistTuples = 0;
	private static int DtContainingFreqNonDistTuples = 0;
	private static int nonDtContainingFreqNonDistTuples = 0;
	
	private static int DtContainingInfreqTuples = 0;
	private static int nonDtContainingInfreqTuples = 0;
	
	private static Set<Integer> trContainingDistSemiFCI = new TreeSet<Integer>();
	private static Set<Integer> trContainingSemiFCI = new TreeSet<Integer>();
	private static Set<Integer> movableNonDTs = new TreeSet<Integer>();
	
	private static Set<Integer> toBeRemoved;
	
	// Basic classification
	public static void classifyMovableNonDTs(Cluster cluster, WorkloadBatch wb) {
		
		toBeRemoved = new TreeSet<Integer>();
		
		//Find DTs and movable non-DTs
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			Transaction tr = wb.getTransaction(h.getId());
			
			if(tr.isDt()) { // DTs
				++DT;
				tr.setTr_class("red");				
				
			} else {// Finding movable non-DTs				
				TransactionClassifier.findAllNonDT(wb, tr);
			}			
		}
		
		TransactionClassifier.remove(cluster, wb);
		wrapup(wb);
	}
	
	public static void init() {
		
		incMine_learner = new IncMine();
		
		// Configure the learner
		incMine_learner.minSupportOption.setValue(0.05d);
		incMine_learner.relaxationRateOption.setValue(0.5d);
		incMine_learner.fixedSegmentLengthOption.setValue(1000); //1000		
		incMine_learner.windowSizeOption.setValue(10);
		incMine_learner.maxItemsetLengthOption.setValue(-1);
		incMine_learner.resetLearning();
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
        	incMine_learner.trainOnInstance(stream.nextInstance());            
        }		
	}
	
	// DSM (FD) - Transactions containing frequent distributed tuple pairs only
	public static void classifyFrequentDT(Cluster cluster, WorkloadBatch wb) {				
		// Initialisation
		init();
		
		//Find the list of semi-FCI
		mine(cluster, wb);		
		System.out.println(incMine_learner);
	
		//Find the list of distributed semi-FCI (Frequent Closed Itemsets)
		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();
		ArrayList<List<Integer>> dSemiFCIList = new ArrayList<List<Integer>>();
		
		for(SemiFCI semiFCI : incMine_learner.getFCITable()){
			System.out.println("@ "+semiFCI.getItems());
			
			if(semiFCI.getItems().size() > 1){
				System.out.println("@ "+semiFCI.getItems());
				semiFCIList.add(semiFCI.getItems());
				
				if(isDistributedFCI(cluster, semiFCI.getItems()))
					dSemiFCIList.add(semiFCI.getItems());
			}
        }
		
		Global.LOGGER.info("Total "+dSemiFCIList.size()+" distributed semi-frequent closed data-tuple sets have been identified.");
		
		//Find the transactions containing distributed frequent semi-FCI tuple pairs
		toBeRemoved = new TreeSet<Integer>();
		
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			
			Transaction tr = wb.getTransaction(h.getId());
			
			// Following Figure 3 from BDC'2014 paper
			if(tr.isDt()) { // DTs				
				++DT;
				
				if(isFrequent(tr, semiFCIList)) { // Frequent
					
					if(containsDistSemiFCI(tr, dSemiFCIList)) { // Contains distributed frequent tuple pairs
						++DtContainingFreqDistTuples;
						trContainingDistSemiFCI.add(tr.getTr_id());
						
					} else { // Contains non-distributed frequent tuple pairs
						++DtContainingFreqNonDistTuples;
						trContainingSemiFCI.add(tr.getTr_id());
						
					}
					
				} else { // Infrequent
					++DtContainingInfreqTuples;
					toBeRemoved.add(tr.getTr_id());
				}
				
			} else { // non-DTs
				
				++nonDT;
				
				if(isMovable(wb, tr)) { // Movable
					
					++movableNonDt;
					
					if(isFrequent(tr, semiFCIList)) { // Frequent -- Contains non-distributed frequent tuple pairs
						++nonDtContainingFreqNonDistTuples;
						trContainingSemiFCI.add(tr.getTr_id());
						
					} else { // Infrequent
						++nonDtContainingInfreqTuples;
						toBeRemoved.add(tr.getTr_id());
					}
					
				} else { // Non-movable
					toBeRemoved.add(tr.getTr_id());
				}
			}
		} // end-for()	
		
		TransactionClassifier.remove(cluster, wb);
		wrapup(wb);
	}
	
	// DSM - (FD+FND)
	public static void classifyMovableFDFND(Cluster cluster, WorkloadBatch wb) {
		
		toBeRemoved = new TreeSet<Integer>();
		
		init();
		
		//Find the list of semi-FCI
		mine(cluster, wb);		
		System.out.println(incMine_learner);
	
		//Find the list of semi-FCI
		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();
		
		for(SemiFCI semiFCI : incMine_learner.getFCITable()){
			System.out.println("@ "+semiFCI.getItems());
			
			if(semiFCI.getItems().size() > 1){
				System.out.println("@ "+semiFCI.getItems());
				semiFCIList.add(semiFCI.getItems());				
			}
        }
		
		Global.LOGGER.info("Total "+semiFCIList.size()+" semi-frequent closed data tuple sets have been identified.");
		
		//Find the transactions containing semi-FCI		
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			
			Transaction tr = wb.getTransaction(h.getId());
			
			// Infrequent Transactions
			if(!isFrequent(tr, semiFCIList)){
				
				if(!toBeRemoved.contains(tr.getTr_id())) {
					++DtContainingInfreqTuples;				
					toBeRemoved.add(tr.getTr_id());
				}
				
			} else { // Frequent Transactions
											
				if(tr.isDt()) { // Distributed Transactions
					
					tr.setTr_class("red");
					++DT;
					
					trContainingSemiFCI.add(tr.getTr_id());	

				} else { // Non-Distributed Transactions
				
					TransactionClassifier.findAllNonDT(wb, tr);
					
				}
			} //end-if-else()
		} //end-for()
				
		TransactionClassifier.remove(cluster, wb);
		wrapup(wb);
	}			
	
	// Returns true if a transaction contains any of the mined semi-FCI
	private static boolean isFrequent(Transaction transaction, ArrayList<List<Integer>> semiFCIList){
		for(List<Integer> semiFCI : semiFCIList){
			if(transaction.getTr_dataSet().containsAll(semiFCI))
				return true;
		}
		
		return false;
	}
	
	// Returns true if a transaction contains any of the mined distributed semi-FCI
	private static boolean containsDistSemiFCI(Transaction tr, ArrayList<List<Integer>> dSemiFCIList){
		for(List<Integer> dSemiFCI : dSemiFCIList){
			if(tr.getTr_dataSet().containsAll(dSemiFCI))
				return true;
		} 
		
		return false;
	}	
	
	// Returns true if a FCI is distributed between two or more physical servers
	private static boolean isDistributedFCI(Cluster cluster, List<Integer> semiFCI){
		Data data;
		Set<Integer> nidSet = new TreeSet<Integer>();
		
		for(Integer d : semiFCI){
			data = cluster.getData(d);
			
			nidSet.add(data.getData_server_id());
			if(nidSet.size() > 1)
				return true;
		}
				
		return false;
	}
		
	private static boolean isMovable(WorkloadBatch wb, Transaction tr) {
		
		for(Integer d : tr.getTr_dataSet()) {			
			SimpleVertex v = wb.hgr.getVertex(d);
			
			for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {				
				Transaction incidentTr = wb.getTransaction(h.getId());
				
				switch(Global.trClassificationStrategy) {
				
					case "basic": // Movable non-DT						
						if(incidentTr.isDt() && !incidentTr.equals(tr))					
							return true;
						
						break;
					
					case "fd": // Frequent DTs containing distributed tuple pairs
						if(incidentTr.isDt() && !incidentTr.equals(tr) 
								&& trContainingDistSemiFCI.contains(incidentTr.getTr_id()))
							return true;
						
						break;
					
					case "fdfnd":  // Movable non-DT containing frequent non-DT tuples						
						if(!incidentTr.isDt() && !incidentTr.equals(tr) 
								&& trContainingSemiFCI.contains(incidentTr.getTr_id())) 
							return true;
						
						break;
				}
			}
		}
		
		return false;
	}
	
	// Find all movable and non-movable non-DTs
	private static void findAllNonDT(WorkloadBatch wb, Transaction tr) {
		
		// Evaluating movable and non-movable non-DTs
		if(isMovable(wb, tr)) {
				
			tr.setTr_class("orange");
			++movableNonDt;
			++nonDT;
				
			movableNonDTs.add(tr.getTr_id());
			
		} else {
			
			tr.setTr_class("green");
			++nonDT;
			
			// Remove
			toBeRemoved.add(tr.getTr_id());
			
		} //end-if-else()
	}
	
	// Remove the non-movable non-DT edges from Graph and Hypergraph
	private static void remove(Cluster cluster, WorkloadBatch wb) {		
		
		for(Integer i : toBeRemoved) {
			
			// Remove from Hypergraph
			SimpleHEdge h = wb.hgr.getHEdge(i);
			wb.hgr.removeHEdge(h);
			
			if(Global.compressionBeforeSetup && Sword.hCut.contains(h))
				Sword.hCut.remove(h);
		}				
	}
	
	private static void wrapup(WorkloadBatch wb) {
		
		wb.set_tr_nums(wb.hgr.getEdgeCount() - DtContainingInfreqTuples - nonDT);		
		wb.set_old_ndt_nums(DtContainingInfreqTuples + nonDT);
				
		Global.LOGGER.info("Classified "+DT+" transactions as purely distributed !!!");	
		Global.LOGGER.info("Classified "+nonDT+" transactions as non-distributed !!!");
		Global.LOGGER.info("Classified "+movableNonDt+" non-distributed transactions as movable !!!");

		Global.LOGGER.info("Classified "+DtContainingFreqDistTuples+" distributed transactions containing frequent distributed tuple pairs.");
		Global.LOGGER.info("Classified "+DtContainingFreqNonDistTuples+" distributed transactions containing frequent but non-distributed tuple pairs.");
		Global.LOGGER.info("Classified "+nonDtContainingFreqNonDistTuples+" non-distributed transactions containing frequent but non-distributed tuple pairs (movable).");
		
		
		Global.LOGGER.info("Classified "+DtContainingInfreqTuples+" distributed transactions as infrequent.");
		Global.LOGGER.info("Classified "+nonDtContainingInfreqTuples+" non-distributed transactions as infrequent (movable).");
		
		Global.LOGGER.info("Total "+toBeRemoved.size()+" purely non-distributed transactions have been removed from the workload hypergraph.");
		
		DT = 0;
		movableNonDt = 0;
		nonDT = 0;
		
		DtContainingFreqDistTuples = 0;
		DtContainingFreqNonDistTuples = 0;
		nonDtContainingFreqNonDistTuples = 0;
		
		DtContainingInfreqTuples = 0;
		nonDtContainingInfreqTuples = 0;
	}
}