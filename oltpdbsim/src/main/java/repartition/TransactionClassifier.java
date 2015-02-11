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

	private static IncMine learner;
	private static ZakiFileStream stream;

	private static int tr_red = 0;
	private static int tr_orange = 0;
	private static int tr_green = 0;
	private static int tr_old = 0;
	private static int old_dti_sum = 0;
	private static int old_ndti_sum = 0;	
	
	private static Set<Integer> frequent_dsfci = new TreeSet<Integer>();
	private static Set<Integer> frequent_sfci = new TreeSet<Integer>();
	private static Set<Integer> movable = new TreeSet<Integer>();
	
	private static Set<Integer> toBeRemoved;
	
	// Removal of old transactions
	public static void removeOldTransactions(Cluster cluster, WorkloadBatch wb) {
		
		toBeRemoved = new TreeSet<Integer>();
		
		//Find the distributed movable transactions
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			Transaction tr = wb.getTransaction(h.getId());
			
			if(tr.isExpired()) {				
				++tr_old;
				toBeRemoved.add(tr.getTr_id());
			}
		}
		
		TransactionClassifier.remove(cluster, wb);		
		wrapup(wb);
	}
	
	// Basic classification
	public static void classifyMovableDTs(Cluster cluster, WorkloadBatch wb) {
		
		toBeRemoved = new TreeSet<Integer>();
		
		//Find the distributed movable transactions
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			Transaction tr = wb.getTransaction(h.getId());
			
			if(!tr.isExpired()) {
				if(tr.isDt()) { // Distributed Transactions
					tr.setTr_class("red");
					++tr_red;
					
				} else {// Non-Distributed Transactions
					
					TransactionClassifier.findAllNonDT(wb, tr);
				}
			} else {
				++tr_old;
				toBeRemoved.add(tr.getTr_id());
			}
		}
		
		TransactionClassifier.remove(cluster, wb);
		wrapup(wb);
	}
	
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
	public static void classifyMovableFD(Cluster cluster, WorkloadBatch wb) {		
		
		toBeRemoved = new TreeSet<Integer>();
		
		init();
		
		//Find the list of semi-FCI
		mine(cluster, wb);		
		//System.out.println(this.learner);
	
		//Find the list of distributed semi-FCI
		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();
		ArrayList<List<Integer>> dsfciList = new ArrayList<List<Integer>>();
		
		for(SemiFCI semiFCI : learner.getFCITable()){
			//System.out.println("@ "+semiFCI.getItems());
			
			if(semiFCI.getItems().size() > 1){
				//System.out.println("@ "+semiFCI.getItems());
				semiFCIList.add(semiFCI.getItems());
				
				if(isDistributedFCI(cluster, semiFCI.getItems()))
					dsfciList.add(semiFCI.getItems());
			}
        }
		
		Global.LOGGER.info("Total "+dsfciList.size()+" distributed semi-frequent closed data tuple sets have been identified.");
		
		//Find the transactions containing distributed semi-FCI
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			
			Transaction tr = wb.getTransaction(h.getId());
			
			// Infrequent Transactions
			if(!isFrequent(tr, semiFCIList)){
				
				if(!toBeRemoved.contains(tr.getTr_id())) {
					++tr_old;
					toBeRemoved.add(tr.getTr_id());
				}
				
			} else { // Frequent Transactions
											
				if(tr.isDt()) { // Distributed Transactions
					
					// Distributed Transactions containing Distributed Semi-FCI
					if(containsDSFCI(tr, dsfciList)){
						
						tr.setTr_class("red");
						++tr_red;
						
						frequent_dsfci.add(tr.getTr_id());
						
					} else { // Distributed Transactions containing Non-Distributed Semi-FCI
						
						if(!toBeRemoved.contains(tr.getTr_id())) {
							++tr_old;
							toBeRemoved.add(tr.getTr_id());
						}
						
					} //end-if-else()
					
				} else { // Non-Distributed Transactions
				
					TransactionClassifier.findAllNonDT(wb, tr);
					
				}
			} //end-if-else()
		} //end-for()
		
		TransactionClassifier.remove(cluster, wb);
		wrapup(wb);
	}
	
	// DSM - (FD+FND)
	public static void classifyMovableFDFND(Cluster cluster, WorkloadBatch wb) {
		
		toBeRemoved = new TreeSet<Integer>();
		
		init();
		
		//Find the list of semi-FCI
		mine(cluster, wb);		
		//System.out.println(this.learner);
	
		//Find the list of semi-FCI
		ArrayList<List<Integer>> semiFCIList = new ArrayList<List<Integer>>();
		
		for(SemiFCI semiFCI : learner.getFCITable()){
			//System.out.println("@ "+fci.getItems());
			
			if(semiFCI.getItems().size() > 1){
				//System.out.println("@ "+fci.getItems());
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
					++tr_old;				
					toBeRemoved.add(tr.getTr_id());
				}
				
			} else { // Frequent Transactions
											
				if(tr.isDt()) { // Distributed Transactions
					
					tr.setTr_class("red");
					++tr_red;
					
					frequent_sfci.add(tr.getTr_id());	

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
	private static boolean containsDSFCI(Transaction tr, ArrayList<List<Integer>> dsfciList){
		for(List<Integer> dSemiFCI : dsfciList){
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
			
			for(SimpleHEdge nh : wb.hgr.getIncidentEdges(v)) {				
				Transaction incident_tr = wb.getTransaction(nh.getId());
				
				switch(Global.trClassificationStrategy) {
				
					case "basic":
						
						if(incident_tr.isDt() && !incident_tr.equals(tr))					
							return true;
						
						break;
					
					case "fd":
				
						if(incident_tr.isDt() && !incident_tr.equals(tr) 
								&& frequent_dsfci.contains(incident_tr.getTr_id()))
							return true;
						
						break;
					
					case "fdfnd":
						
						if(incident_tr.isDt() && !incident_tr.equals(tr) 
								&& frequent_sfci.contains(incident_tr.getTr_id()))
							return true;
						
						break;	
				}
			}
		}
		
		return false;
	}
	
	// Find all movable and non-movable non-DTs
	private static void findAllNonDT(WorkloadBatch wb, Transaction tr) {
		
		// Evaluating Movable and Non-Movable Transactions
		if(isMovable(wb, tr)) {
				
				tr.setTr_class("orange");
				++tr_orange;
				++tr_green;
				
				movable.add(tr.getTr_id());
			
		} else {
			
			tr.setTr_class("green");
			++tr_green;
			
			// Remove
			toBeRemoved.add(tr.getTr_id());
			
		} //end-if-else()
	}
	
	// Remove the nonDT non-movable edges from Graph and Hypergraph
	private static void remove(Cluster cluster, WorkloadBatch wb) {
		
		for(Integer i : movable) {			
			Transaction tr = wb.getTransaction(i);
			tr.calculateDTImapct();
			old_ndti_sum += tr.getTr_dtImpact();
		}
		
		for(Integer i : toBeRemoved) {
			
			Transaction tr = wb.getTransaction(i);
			tr.calculateDTImapct();
			
			if(tr.isDt())				
				old_dti_sum += tr.getTr_dtImpact();
			else
				old_ndti_sum += tr.getTr_dtImpact();
			
			// Remove from Hypergraph
			SimpleHEdge h = wb.hgr.getHEdge(i);
			wb.hgr.removeHEdge(h);
			
			if(Global.compressionBeforeSetup && wb.sword.hCut.contains(h))
				wb.sword.hCut.remove(h);
				
			if(Global.dataMigrationStrategy.equals("methodX"))
				wb.methodX.updateAssociation(cluster, tr, true);
		}
		
		Global.LOGGER.info("Total "+toBeRemoved.size()+" old and purely non-distributed transactions have removed.");
	}
	
	private static void wrapup(WorkloadBatch wb) {
		
		wb.set_tr_nums(wb.hgr.getEdgeCount() - tr_old - tr_green);		
		wb.set_old_ndt_nums(tr_old + tr_green);
		wb.set_old_dti_sum(old_dti_sum);
		wb.set_old_ndti_sum(old_ndti_sum);
				
		Global.LOGGER.info("Classified "+tr_red+" transactions as purely distributed !!!");		
		Global.LOGGER.info("Classified "+tr_green+" transactions as non distributed !!!");			
		Global.LOGGER.info("Classified "+tr_orange+" transactions as non distributed but movable !!!");
		Global.LOGGER.info("Classified "+tr_old+" transactions as old/expired.");
		
		tr_red = 0;
		tr_orange = 0;
		tr_green = 0;
		tr_old = 0;
		old_dti_sum = 0;
		old_ndti_sum = 0;
	}
}