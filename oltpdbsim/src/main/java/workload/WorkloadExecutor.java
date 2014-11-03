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
	    
    static int[] trType;
    static int simTimeInHours;
    static double clock;
    
    static int currentDT;
    static int initialDT;
    static int marginDT;
    
    static boolean initial;
    static boolean changeDetected;
    
	static RandomVariateGen genArr;
	static RandomVariateGen genServ;

	static LinkedList<Transaction> waitList = new LinkedList<Transaction> ();
	static LinkedList<Transaction> servList = new LinkedList<Transaction> ();

	static Tally transactionWaits = new Tally("Waiting times");
	static Accumulate totalWait = new Accumulate("Size of queue");

	// Statistic collectors
	static int batch_total_transactions;
	static double batch_total_response_time;	
	
	// Seed
	static long[] seed = new long[10];
	
    public WorkloadExecutor() {    	

        simTimeInHours = 0;
        clock = 0.0;
        currentDT = 0;
        initialDT = 0;
        
        initial = true;
        changeDetected = false;
        
        MRG32k3a rand = new MRG32k3a();                
        for(int i = 0; i < 5; i++)
        	seed[i] = (long) Global.repeated_runs + i;

        rand.setSeed(seed);
        
        genArr = new ExponentialGen(rand, Global.meanInterArrivalTime); // mean inter arrival rate = 1/lambda        
		genServ = new ExponentialGen(rand, Global.meanServiceTime); // mean service time = 1/mu        
        
        batch_total_transactions = 0;
        batch_total_response_time = 0.0;
    }
	
	public static Transaction streamOneTransaction(Database db, Cluster cluster, 
			Workload wrl, WorkloadBatch wb) {
		
		int t = 0, tr_id;
		Transaction tr = null;
		Set<Integer> trTupleSet = null;
		Set<Integer> trDataSet = null;
		int type = trDistribution.sample();
		
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
			++currentDT;
			
			wb.incWrl_dt_nums();
			wb.incWrl_totalTransaction();
			
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
    		
    		batch_total_response_time += round_avg_response;
    		
    		tr.setTr_response_time(round_avg_response);
    		tr.setProcessed(true); 
    		
    		//System.out.println("-->> Processed T"+tr.getTr_id()+" within "+tr.getTr_response_time());
    		
    		++batch_total_transactions;
    		
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
		Global.LOGGER.info("Total "+batch_total_transactions+" transactions have processed by the Transaction Coordinator of them "+Global.global_trSeq+" are unique.");
		Global.LOGGER.info("Total time: "+(Sim.time() / 3600)+" Hours");		
					
		// Statistic preparation, calculation, and reporting
		if(!Global.staticRepartitioning)
			++Global.repartitioningCycle;
		
		cluster.updateLoad();
		collectStatistics(cluster, wb);
	}
	
	public static void collectStatistics(Cluster cluster, WorkloadBatch wb) {
		wb.setWrl_totalTransaction(Global.global_trSeq);
		wb.calculateDTI(cluster);
		wb.calculateThroughput(batch_total_transactions);
		wb.calculateResponseTime(batch_total_transactions, batch_total_response_time);
		
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
		
		// Set DT margin
		if(Sim.time() >= 3600 && WorkloadExecutor.initial) {						
			WorkloadExecutor.initialDT = wb.getWrl_dt_nums();
			WorkloadExecutor.marginDT = (int) ((WorkloadExecutor.initialDT * Global.dtChangePercentage) 
					+ WorkloadExecutor.initialDT);

			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Setting a DT margin of "+WorkloadExecutor.marginDT+" distributed transactions.");
			Global.LOGGER.info(WorkloadExecutor.currentDT+" distributed transactions present in the current workload.");
			
			// Statistic preparation, calculation, and reporting
			cluster.updateLoad();								
			WorkloadExecutor.collectStatistics(cluster, wb);
			
			WorkloadExecutor.initial = false;
		}
		
		// % increase in DT detection
		if(!WorkloadExecutor.initial && !WorkloadExecutor.changeDetected && Global.staticRepartitioning) {				
			
			if(WorkloadExecutor.currentDT >= WorkloadExecutor.marginDT) {
	
				++Global.repartitioningCycle;
				WorkloadExecutor.changeDetected = true;
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");
				Global.LOGGER.info((Global.dtChangePercentage * 100) + "% increase in DT detected !!!");
				Global.LOGGER.info("Number of DT have increased from "+WorkloadExecutor.initialDT+" to "+WorkloadExecutor.currentDT+".");
				Global.LOGGER.info(WorkloadExecutor.currentDT + " distributed transactions present in the current workload stream of "+Global.global_trSeq+" unique transactions.");
				Global.LOGGER.info(""+WorkloadExecutor.batch_total_transactions+" transaction have processed so far.");
				Global.LOGGER.info("Current simulation time: "+Sim.time()/3600+" hrs");					
								
				//Global.LOGGER.info("Vertex Count = "+wb.hgr.getVertexCount());
				//Global.LOGGER.info("Data Count = "+Global.global_dataCount);				
				
				if((Global.workloadAware && Global.incrementalRepartitioning) 
						|| (Global.workloadAware && Global.staticRepartitioning)) {

					Global.LOGGER.info("-----------------------------------------------------------------------------");
					Global.LOGGER.info("Starting database repartitioning ...");
					
					//Perform Data Stream Mining to find the transactions containing Distributed Semi-Frequent Closed Itemsets (tuples)		
					Global.LOGGER.info("Identifying most frequently occurred transactions ...");
					
					switch(Global.trClassificationStrategy) {
						case "basic":
							TransactionClassifier.classifyMovableDTs(cluster, wb);
							break;
							
	//					case "fd":
	//						TransactionClassifier.classifyMovableFD(cluster, wb);
	//						break;
	//						
	//					case "fdfnd":
	//						TransactionClassifier.classifyMovableFDFND(cluster, wb);
	//						break;						
						}					
					
					// Assign shadow data id and generate workload and fix files		
					Global.LOGGER.info("Total "+wb.hgr.getEdgeCount()+" transactions containing "+wb.hgr.getVertexCount()+" data objects have identified for repartitioning.");
					Global.LOGGER.info("-----------------------------------------------------------------------------");
					
					// Generate workload file
					boolean empty = false;
					try {
						empty = WorkloadBatchProcessor.generateWorkloadFile(cluster, wb);
					} catch (IOException e) {
						e.printStackTrace();
					}
					
					if(!empty) {
						
						int partitions = Global.partitions;
						Global.LOGGER.info("Starting partitioner to repartition the workload into "+partitions+" clusters ...");	
						
						// Perform hyper-graph/graph/compressed hyper-graph partitioning			 
						MinCut.runMinCut(wb, partitions, true);
	
						// Mapping cluster id to partition id
						Global.LOGGER.info("Applying data movement strategies for database ("+db.getDb_name()+") ...");					
						try {
							WorkloadBatchProcessor.processPartFile(cluster, wb, partitions);
						} catch (IOException e) {
							e.printStackTrace();
						}					
						
						// Perform data movement
						DataMovement.performDataMovement(cluster, wb, Global.dataMigrationStrategy, Global.workloadRepresentation);					
						
						// Update Node level load
						cluster.updateLoad();
						
						// Display the Cluster status
						cluster.show();
						
						Global.LOGGER.info("=======================================================================================================================");
						
					} else {
						
						DataMovement.setEnvironment(cluster, wb);
						DataMovement.wrappingUp(true, Global.dataMigrationStrategy, cluster, wb, Global.workloadRepresentation);
						
						// Update Node level load
						cluster.updateLoad();								
	
						// Display the Cluster status
						cluster.show();
						
						Global.LOGGER.info("No changes are required for database ("+db.getDb_name()+")");
						Global.LOGGER.info("Repartitioning run round aborted for database ("+db.getDb_name()+")");
						Global.LOGGER.info("=======================================================================================================================");
					}
					
					wrapup(cluster, wb);					
					Global.staticRepartitioning = false;
					
				} 
				
				// Actions for SWORD
				if(Global.compressionBeforeSetup) { 
					Global.LOGGER.info("SWORD actions ...");
	
					wrapup(cluster, wb);
				}

				WorkloadExecutor.changeDetected = false;
			}
		}
	}
	 
	private static void wrapup(Cluster cluster, WorkloadBatch wb) {
		// Show batch status in console	
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total "+WorkloadExecutor.batch_total_transactions+" transactions have processed by the Transaction Coordinator of them "+Global.global_trSeq+" are unique.");		
					
		// Statistic preparation, calculation, and reporting
		cluster.updateLoad();								
		WorkloadExecutor.collectStatistics(cluster, wb);
		
		// Setting new DT margin
		Global.LOGGER.info("Number of DTs before repartitioning "+WorkloadExecutor.currentDT);
		Global.LOGGER.info("Number of DTs after repartitioning "+wb.getWrl_dt_nums());
		
		if(Global.incrementalRepartitioning) {
			WorkloadExecutor.currentDT = wb.getWrl_dt_nums();
			WorkloadExecutor.initialDT = WorkloadExecutor.currentDT;
			WorkloadExecutor.marginDT = (int) ((WorkloadExecutor.initialDT * Global.dtChangePercentage) 
					+ WorkloadExecutor.currentDT);
			
			Global.LOGGER.info("Setting a new DT margin of "+WorkloadExecutor.marginDT+" distributed transactions.");
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