/* *****************************************************************************
 * Simulation of a simple M/M/1 queue (Grocery).
 * Class: Simulation - Modeling and Performance Analysis 
 *        with Discrete-Transaction Simulation
 *        RWTH Aachen, Germany, 2007
 *        
 * Author: Dr. Mesut Günes, guenes@cs.rwth-aachen.de        
 * 
 * Notice: 
 * This code is based on the example presented in the very nice book of 
 * Jerry Banks, John S. Carson II, Barry L. Nelson, David M. Nicol: 
 * Discrete-Transaction System Simulation, Fourth Edition, Prentice Hall, 2005.
 * 
 * However, the code is not exactly the same ;-)
 *  
 ******************************************************************************/
package main.java.workload;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;

import umontreal.iro.lecuyer.randvar.ExponentialGen;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.MRG32k3a;
import umontreal.iro.lecuyer.simevents.Accumulate;
import umontreal.iro.lecuyer.simevents.Event;
import umontreal.iro.lecuyer.simevents.Sim;
import umontreal.iro.lecuyer.stat.Tally;
import main.java.cluster.Cluster;
import main.java.db.Database;
import main.java.entry.Global;
import main.java.metric.Metric;
import main.java.repartition.DataMovement;
import main.java.repartition.MinCut;
import main.java.repartition.TransactionClassifier;
import main.java.repartition.WorkloadBatchProcessor;

public class WorkloadExecutor {

	static EnumeratedIntegerDistribution trDistribution;

	static RandomVariateGen genArr;
	static RandomVariateGen genServ;

	static LinkedList<Transaction> waitList = new LinkedList<Transaction> ();
	static LinkedList<Transaction> servList = new LinkedList<Transaction> ();

	static Tally transactionWaits = new Tally("Waiting times");
	static Accumulate totalWait = new Accumulate("Size of queue");

	// State observation and statistic collectors
	static boolean initial = true;
    static boolean changeDetected = false;
    static int dtCount = 0;
    static int dtMargin = 0;
	static int last_observed_dtCount = 0;	
	static int total_trFrequency = 0;
	static double total_response_time = 0.0;	
	
	// Seed
	static long[] seed = new long[10];
	
    public WorkloadExecutor() {
        
        MRG32k3a rand = new MRG32k3a();                
        for(int i = 0; i < 5; i++)
        	seed[i] = (long) Global.repeated_runs + i;

        rand.setSeed(seed);
        
        genArr = new ExponentialGen(rand, Global.meanInterArrivalTime); // mean inter arrival rate = 1/lambda        
		genServ = new ExponentialGen(rand, Global.meanServiceTime); // mean service time = 1/mu
    }
	
	public static Transaction streamOneTransaction(Database db, Cluster cluster, 
			Workload wrl, WorkloadBatch wb) {

		Set<Integer> trTupleSet = null;
		Set<Integer> trDataSet = null;
		
		int t = 0, tr_id;
		int type = trDistribution.sample();

		Transaction tr = null;
		
		if(!wb.getTrBuffer().containsKey(type))
			wb.getTrBuffer().put(type, new ArrayList<Integer>());
		
		if(!wb.getTrMap().containsKey(type))
			wb.getTrMap().put(type, new TreeMap<Integer, Transaction>());		
		
		// new
		double rand_val = Global.rand.nextDouble();
		
		// Transaction birth
		if(wb.getTrBuffer().get(type).isEmpty() || rand_val <= Global.workloadChangeProbability) {
			//System.out.println("-->> inserting new transaction ...");
			trTupleSet = wrl.getTrTupleSet(db, type);
			trDataSet = Workload.getTrDataSet(db, cluster, wb, trTupleSet);
			
			++Global.global_trSeq;
			tr = new Transaction(Global.global_trSeq, type, trDataSet, Sim.time());
			
			// Add into Transaction buffer
			wb.getTrBuffer().get(type).add(tr.getTr_id());

			// Add the newly created Transaction in the Workload Transaction map	
			wb.getTrMap().get(type).put(tr.getTr_id(), tr);
			
		// Transaction repetition	
		} else {
			//System.out.println("-->> repeating existing transaction ..."); 
			t = Global.rand.nextInt(wb.getTrBuffer().get(type).size());			
			tr_id = wb.getTrBuffer().get(type).get(t);
			
			tr = wb.getTrMap().get(type).get(tr_id);
			tr.incTr_frequency();
			tr.setTimestamp(Sim.time());
			tr.setProcessed(false);

			if(!tr.isTemporal()) {
				tr.decTr_temporalWeight();
				tr.setTemporal(true);
			}			
		}
				
		// Transaction death
		if(rand_val <= Global.workloadChangeProbability) {			
			//System.out.println("-->> killing existing transaction ...");
			//t = Global.rand.nextInt(wb.getTrBuffer().get(type).size());			
			//wb.getTrBuffer().get(type).remove(t);
			wb.getTrBuffer().get(type).remove(0);			
		}	
		
		tr.calculateSpans(cluster);
		
		if(tr.isDt() && !tr.isVisited()) {			
			++dtCount;
			tr.setVisited(true);		
		}
		
		// Add edges and hyperedge to Workload Graph and Hypergraph
		switch(Global.workloadRepresentation) {
			case "gr":
				wb.addGraphEdges(tr);
				break;
				
			case "hgr":
				wb.addHGraphEdge(tr);
				break;
		}
		
		return tr;
	}
		
	public static void processTransaction(Transaction tr) {
		// Sleep for x milliseconds
    	try {    		
    		//TimeUnit.MILLISECONDS.sleep(tr.getTr_serverSpanCost());
    		Thread.sleep(tr.getTr_serverSpanCost());
    		
    		double response_time = (double)tr.getTr_serverSpanCost() + tr.getTr_waiting_time();    		
    		double avg_response = (response_time + tr.getTr_response_time()) / tr.getTr_frequency();
    		double round_avg_response = Math.round(avg_response * 100.0) / 100.0;
    		
    		total_response_time += round_avg_response;    		
    		total_trFrequency += tr.getTr_frequency();
    		
    		tr.setTr_response_time(round_avg_response);
    		tr.setProcessed(true);    		
    		
    		//System.out.println("-->> Processed T"+tr.getTr_id()+" within "+tr.getTr_response_time());
    		
    		++Global.total_transactions;
    		
    	} catch(Exception e) {
    	    e.printStackTrace();
    	}
	}

	public static void simulate(Database db, Cluster cluster, Workload wrl, WorkloadBatch wb, double timeHorizon) {		
		Sim.init();		
		new EndOfSim().schedule(timeHorizon);
		new Arrival(db, cluster, wrl, wb).schedule(genArr.nextDouble());		
		Sim.start();
		
		//System.out.println (WorkloadExecutor.transactionWaits.report());
		//System.out.println (WorkloadExecutor.totalWait.report());
	}	
	
	// new
	public void execute(Database db, Cluster cluster, Workload wrl) {
		Global.LOGGER.info("=============================================================================");
		Global.LOGGER.info("Streaming transactional workload ...");
		
		trDistribution = new EnumeratedIntegerDistribution(wrl.trTypes, wrl.trProbabilities);		
		trDistribution.reseedRandomGenerator(seed[Global.repeated_runs - 1]); 
		
		// Create a new Workload Batch
		WorkloadBatch wb = new WorkloadBatch(Global.repeated_runs);
		
		// Start simulation
		WorkloadExecutor.simulate(db, cluster, wrl, wb, Global.simulationPeriod);
		
		// Show batch status in console	
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total "+Global.total_transactions+" transactions have processed "
				+ "by the Transaction Coordinator so far and of them "
				+Global.global_trSeq+" are unique.");
		
		Global.LOGGER.info("Total time: "+(Sim.time() / 3600)+" Hours");		
					
		// Statistic preparation, calculation, and reporting		
		collectStatistics(cluster, wb);
	}
	
	public static void collectStatistics(Cluster cluster, WorkloadBatch wb) {
		cluster.updateLoad();
		
		wb.calculateDTI(cluster);		
		wb.calculateThroughput(Global.total_transactions);
		wb.calculateResponseTime(Global.total_transactions, total_response_time);
		wb.calculateAverageTrFreq(Global.total_transactions, total_trFrequency);
		
		Metric.collect(cluster, wb);			
		Metric.report();
		Metric.write();
	}	
} // end-Class

//=======================================================================================
class Arrival extends Event {
	Database db;
	Cluster cluster;
	Workload wrl;
	WorkloadBatch wb;
	
	public Arrival(Database db, Cluster cluster, Workload wrl, WorkloadBatch wb) {
		this.db = db;
		this.cluster = cluster;
		this.wrl = wrl;
		this.wb = wb;
	}

	public void actions() {
		// Next arrival
		new Arrival(this.db, this.cluster, this.wrl, this.wb)
			.schedule(WorkloadExecutor.genArr.nextDouble());
		
		// A Transaction has just arrived
		Transaction tr = WorkloadExecutor
				.streamOneTransaction(this.db, this.cluster, this.wrl, this.wb);
		
		tr.setTr_arrival_time(Sim.time());			
		tr.setTr_service_time(WorkloadExecutor.genServ.nextDouble());
		
		if (WorkloadExecutor.servList.size() > 0) { // Must join the queue.
			
			WorkloadExecutor.waitList.addLast(tr);
			WorkloadExecutor.totalWait.update(WorkloadExecutor.waitList.size());			
			
    		//System.out.println("Added "+tr.getTr_id()+" in the Waiting list("+WorkloadExecutor.waitList.size()+")");
			
		} else { // Starts service
			
			tr.setTr_waiting_time(0.0);
			
			WorkloadExecutor.transactionWaits.add(0.0);			
			WorkloadExecutor.servList.addLast(tr);
			
			//System.out.println("Added "+tr.getTr_id()+" in the Serving list("+WorkloadExecutor.servList.size()+")");			
			
			new Departure().schedule(tr.getTr_service_time());
		}
		
		/**
		 * Three options:
		 * 
		 * 1. Baseline -- no repartitioning hence no data migrations | hourly statistic collection
		 * 		workloadAware = false
		 * 		incrementalRepartitioning = false
		 * 		enableTrClassification = false		
		 * 
		 * 2. Static Repartitioning -- a single repartitioning and data migrations after that hourly statistic collection
		 * 		workloadAware = true
		 * 		incrementalRepartitioning = false
		 * 		enableTrClassification = false
		 * 		workloadRepresentation = GR/HGR/CHG
		 * 		dataMigrationStrategy = Random/MC/MSM		
		 * 
		 * 3. Incremental Repartitioning -- incremental repartitioning based on DT margin
		 * 		workloadAware = true
		 * 		incrementalRepartitioning = true
		 * 		enableTrClassification = true 
		 * 		workloadRepresentation = GR/HGR/CHG
		 * 		transactionClassificationStrategy = Basic/FD/FDFND
		 * 		dataMigrationStrategy = Random/MC/MSM/Sword
		 *  
		 */
		
		if(Global.workloadAware) {
			
			
		} else {
			
			// Hourly statistic collection
			if(Sim.time() >= Global.nextCollection) {
				// Statistic preparation, calculation, and reporting
				wb.set_tr_nums(wb.hgr.getEdgeCount());
				WorkloadExecutor.collectStatistics(cluster, wb);
				Global.nextCollection += 3600.0;
			}
		}
		
//		// Setting initial DT margin
//		if(Sim.time() >= Global.initialDetectionTime && WorkloadExecutor.initial) {
//			
//			WorkloadExecutor.dtMargin = (int) ((WorkloadExecutor.dtCount * Global.percentageChangeDt) 
//					+ WorkloadExecutor.dtCount);
//
//			Global.LOGGER.info("-----------------------------------------------------------------------------");
//			Global.LOGGER.info("Setting a DT margin of "+WorkloadExecutor.dtMargin
//					+" distributed transactions.");
//			Global.LOGGER.info(WorkloadExecutor.dtCount
//					+" distributed transactions present in the current workload of total "
//					+Global.total_transactions+" transaction of them "
//					+Global.global_trSeq+" are unique.");
//			
//			// Statistic preparation, calculation, and reporting
//			wb.set_tr_nums(wb.hgr.getEdgeCount());
//			WorkloadExecutor.collectStatistics(cluster, wb);			
//			
//			WorkloadExecutor.last_observed_dtCount += WorkloadExecutor.dtCount;
//			WorkloadExecutor.initial = false;
//		}
//		
//		// Detecting % increase in DT
//		if(!WorkloadExecutor.initial && WorkloadExecutor.dtCount >= WorkloadExecutor.dtMargin)			
//			WorkloadExecutor.changeDetected = true;
//		
//		if(WorkloadExecutor.changeDetected) {				
//			Global.LOGGER.info("-----------------------------------------------------------------------------");
//			Global.LOGGER.info((Global.percentageChangeDt * 100) + "% increase in DT detected !!!");
//
//			Global.LOGGER.info("Number of DT have increased from "
//					+WorkloadExecutor.last_observed_dtCount+" to "+WorkloadExecutor.dtCount+".");
//			
//			Global.LOGGER.info("Total "+Global.total_transactions+" transactions have processed "
//					+ "by the Transaction Coordinator so far and of them "
//					+Global.global_trSeq+" are unique.");
//
//			Global.LOGGER.info("Current simulation time: "+Sim.time()/3600+" hrs");					
//			
//			
//			if(Global.workloadAware && Global.incrementalRepartitioning) {
//
//				++Global.repartitioningCycle;				
//				
//				Global.LOGGER.info("-----------------------------------------------------------------------------");
//				Global.LOGGER.info("Starting database repartitioning ...");
//				
//				//Perform Data Stream Mining to find the transactions containing Distributed Semi-Frequent Closed Itemsets (tuples)		
//				Global.LOGGER.info("Identifying most frequently occurred transactions ...");
//				
//				switch(Global.trClassificationStrategy) {
//					case "basic":
//						Global.LOGGER.info("Discarding transactions older than last one hour ...");
//						TransactionClassifier.classifyMovableDTs(cluster, wb);
//						break;
//						
//					case "fd":
//						Global.LOGGER.info("Discarding transactions which are not frequent ...");
//						TransactionClassifier.classifyMovableFD(cluster, wb);
//						break;
//						
//					case "fdfnd":
//						Global.LOGGER.info("Discarding transactions which are not frequent ...");
//						TransactionClassifier.classifyMovableFDFND(cluster, wb);
//						break;						
//				}
//				
//				Global.LOGGER.info("Total "+wb.hgr.getEdgeCount()+" transactions containing "
//						+wb.hgr.getVertexCount()+" data objects have identified for repartitioning.");
//				Global.LOGGER.info("-----------------------------------------------------------------------------");
//				
//				// Generate workload file
//				boolean empty = false;
//				try {
//					empty = WorkloadBatchProcessor.generateWorkloadFile(cluster, wb);
//				} catch (IOException e) {
//					e.printStackTrace();
//				}
//				
//				if(!empty) {
//					
//					int partitions = Global.partitions;
//					Global.LOGGER.info("Starting partitioner to repartition the workload into "
//							+partitions+" clusters ...");	
//					
//					// Perform hyper-graph/graph/compressed hyper-graph partitioning			 
//					MinCut.runMinCut(wb, partitions, true);
//
//					// Mapping cluster id to partition id
//					Global.LOGGER.info("Applying data movement strategies for database ("
//							+db.getDb_name()+") ...");
//					
//					try {
//						WorkloadBatchProcessor.processPartFile(cluster, wb, partitions);
//					} catch (IOException e) {
//						e.printStackTrace();
//					}					
//					
//					// Perform data movement
//					DataMovement.performDataMovement(cluster, wb, Global.dataMigrationStrategy, Global.workloadRepresentation);					
//					
//					// Update server-level load statistic and show
//					cluster.updateLoad();
//					//cluster.show();
//					
//					Global.LOGGER.info("=======================================================================================================================");
//					
//				} else {
//											
//					// Update server-level load statistic and show
//					cluster.updateLoad();								
//					//cluster.show();
//					
//					Global.LOGGER.info("No changes are required for database ("+db.getDb_name()+")");
//					Global.LOGGER.info("Repartitioning run round aborted for database ("+db.getDb_name()+")");
//					Global.LOGGER.info("=======================================================================================================================");
//				}
//				
//				wrapup(cluster, wb);
//				
//			} else { // Baseline run
//				
//			} 
//			
//			// Actions for SWORD
//			if(Global.compressionBeforeSetup) { 
//				Global.LOGGER.info("SWORD actions ...");
//
//				wrapup(cluster, wb);
//			}
//
//			WorkloadExecutor.changeDetected = false;
//		}				
	}
	 
	private static void wrapup(Cluster cluster, WorkloadBatch wb) {
		// Statistic preparation, calculation, and reporting								
		WorkloadExecutor.collectStatistics(cluster, wb);
		
		// Setting new DT margin
		Global.LOGGER.info("Number of DTs before repartitioning "+WorkloadExecutor.dtCount);
		Global.LOGGER.info("Number of DTs after repartitioning "+wb.get_dt_nums());
		
		WorkloadExecutor.dtCount = wb.get_dt_nums();
		
		if(Global.dynamicDtMargin) {
			
			WorkloadExecutor.dtMargin = (int) ((WorkloadExecutor.dtCount * Global.percentageChangeDt) 
					+ WorkloadExecutor.dtCount);
			
			Global.LOGGER.info("Setting a new DT margin of "+WorkloadExecutor.dtMargin+".");
		}
	}
	
}

//=======================================================================================
class Departure extends Event {
	public void actions() {
		Transaction transaction = WorkloadExecutor.servList.removeFirst();
		WorkloadExecutor.processTransaction(transaction);
		
		if (WorkloadExecutor.waitList.size() > 0) {
			// Starts service for next one in queue.
			Transaction tr = WorkloadExecutor.waitList.removeFirst();
			tr.setTr_waiting_time((Sim.time() - tr.getTr_arrival_time())/1000); // set in miliseconds

			//System.out.println("Get "+tr.getTr_id()+" from the Waiting list("+WorkloadExecutor.waitList.size()+")");
			
			WorkloadExecutor.totalWait.update(WorkloadExecutor.waitList.size());
			WorkloadExecutor.transactionWaits.add(Sim.time() - tr.getTr_arrival_time());			
			WorkloadExecutor.servList.addLast(tr);

			//System.out.println("Added "+tr.getTr_id()+" in the Serving list("+WorkloadExecutor.servList.size()+")");
			
			new Departure().schedule(tr.getTr_service_time());
		}
	}
}

//=======================================================================================
class EndOfSim extends Event {
	public void actions() {
		Sim.stop();
	}
}