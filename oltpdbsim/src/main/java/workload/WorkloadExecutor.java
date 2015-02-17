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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.db.Database;
import main.java.entry.Global;
import main.java.metric.Metric;
import main.java.metric.PerfMetric;
import main.java.repartition.DataMovement;
import main.java.repartition.MinCut;
import main.java.repartition.TransactionClassifier;
import main.java.repartition.WorkloadBatchProcessor;
import main.java.utils.ValueComparator;

import org.apache.commons.math3.distribution.EnumeratedIntegerDistribution;

import umontreal.iro.lecuyer.randvar.ExponentialGen;
import umontreal.iro.lecuyer.randvar.RandomVariateGen;
import umontreal.iro.lecuyer.rng.MRG32k3a;
import umontreal.iro.lecuyer.simevents.Accumulate;
import umontreal.iro.lecuyer.simevents.Event;
import umontreal.iro.lecuyer.simevents.Sim;
import umontreal.iro.lecuyer.stat.Tally;

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
    static boolean noChangeRequired = false;
    
    static int dt_new = 0;
    static int dt_original = 0;
    
	static double total_response_time = 0.0;	
	
// New improvements------------------------------------------------------------------------------
	static ArrayList<Integer> T = new ArrayList<Integer>();
	static Map<Integer, Double> Time = new HashMap<Integer, Double>();
	static PerfMetric perfm = new PerfMetric();
	
	static double current_idt = 0.0;
	static double threshold_idt = 0.0;	
	
	// Seed
	static long[] seed = new long[10];
	
	// New (January 07, 2015)
	static Map<Integer, Integer> idx = new HashMap<Integer, Integer>();	
	static Set<Integer> toBeRemoved = new TreeSet<Integer>();
	
    public WorkloadExecutor() {
        
        MRG32k3a rand = new MRG32k3a();                
        for(int i = 0; i < 5; i++)
        	seed[i] = (long) Global.repeated_runs + i;

        rand.setSeed(seed);
        
        genArr = new ExponentialGen(rand, Global.meanInterArrivalTime); // mean inter arrival rate = 1/lambda        
		genServ = new ExponentialGen(rand, Global.meanServiceTime); // mean service time = 1/mu
		
		Global.uniqueMax = Global.uniqueMaxFixed 
							- (int)(Global.observationWindow * Global.percentageChangeInWorkload * Global.adjustment);				
    }
	
	public static Transaction streamOneTransaction(Database db, Cluster cluster, 
			Workload wrl, WorkloadBatch wb) {

		Set<Integer> trTupleSet = null;
		Set<Integer> trDataSet = null;
		
		int min = 0, i = 0, n = 0, tr_id = 0;		
		int type = trDistribution.sample();

		Transaction tr = null;
		
		if(!wb.getTrMap().containsKey(type))
			wb.getTrMap().put(type, new TreeMap<Integer, Transaction>());		
		
		// new
		double rand_val = Global.rand.nextDouble();
		int toBeRemovedKey = -1;

/**
 *  Implementing the new Workload Generation model (Finalised as per November 20, 2014 and later improved on February 13-14, 2015)		
 */		
		++Global.global_trCount;
		
		// Transaction birth
		if(wb.getTrMap().get(type).isEmpty() || rand_val <= Global.percentageChangeInWorkload) {
			
			trTupleSet = wrl.getTrTupleSet(db, type);
			trDataSet = Workload.getTrDataSet(db, cluster, wb, trTupleSet);
			
			++Global.global_trSeq;			
			tr = new Transaction(Global.global_trSeq, type, trDataSet, Sim.time());
			
			// Add the newly created Transaction in the Workload Transaction map	
			wb.getTrMap().get(type).put(tr.getTr_id(), tr);
			
// New improvements------------------------------------------------------------------------------
			double initial_period = (double) Global.uniqueMax;
			perfm.Period.put(tr.getTr_id(), initial_period);	
			tr.setTr_period(initial_period);
			
			Time.put(tr.getTr_id(), Sim.time());

		// Transaction repetition and retention of old transaction
		} else {
			
			ArrayList<Integer> idx2_id = new ArrayList<Integer>();
			ArrayList<Integer> idx_value = new ArrayList<Integer>();	
			ArrayList<Integer> uT = new ArrayList<Integer>();
			
			TreeMap<Integer, Integer> idx2 = new TreeMap<Integer, Integer>(new ValueComparator<Integer>(idx));
			idx2.putAll(idx);			
			
			min = Math.min(idx.size(), Global.uniqueMaxFixed);
			
			i = 0;
			Iterator<Entry<Integer, Integer>> itr = idx2.entrySet().iterator();
			while(i < min) {
				idx2_id.add(itr.next().getKey());
				++i;
			}
			
			// Deleting old Transactions
			if(idx2.size() > min) {				
				toBeRemovedKey = idx2.lastKey();
				
				Transaction tr_old = wb.getTransaction(toBeRemovedKey);
				
				tr_old.calculateSpans(cluster);				
				if(tr_old.isDt())
					--dt_new;
				
				wb.removeTransaction(cluster, tr_old);
				idx.remove(toBeRemovedKey);
			}
			
			i = 0;
			while(i < idx2_id.size()) {
				idx_value.add(idx.get(idx2_id.get(i)));
				++i;
			}
			
			i = 0;
			while(i < idx_value.size()) {
				uT.add(T.get(idx_value.get(i) - 1));
				++i;
			}			

			if(uT.size() == 1)
				n = 0;
			else
				n = Global.rand.nextInt(uT.size());
			
			tr_id = uT.get(n);
			
			tr = wb.getTransaction(tr_id);
			tr.setProcessed(false);			
			
// New improvements------------------------------------------------------------------------------
			double prev_period = perfm.Period.get(tr.getTr_id());			
			double prev_time = Time.get(tr.getTr_id());
			
		    double new_period = prev_period * Global.expAvgWt 
		    		+ (Sim.time() - prev_time) * (1 - Global.expAvgWt);
		    
		    perfm.Period.remove(tr.getTr_id());
		    perfm.Period.put(tr.getTr_id(), new_period);
		    tr.setTr_period(new_period);
		    
		    Time.remove(tr.getTr_id());		
		    Time.put(tr.getTr_id(), Sim.time());
		    
		} // end-if-else()
		
		// Calculate latest Span
		tr.calculateSpans(cluster);
		
		if(perfm.Span.containsKey(tr.getTr_id()))
			perfm.Span.remove(tr.getTr_id());
		
		perfm.Span.put(tr.getTr_id(), tr.getTr_serverSpanCost());		
		
		if(tr.isDt()) ++dt_new;
		
		// Create an index entry for each newly created Transaction		
		idx.put(tr.getTr_id(), Global.global_trCount);
		T.add(tr.getTr_id());		
		
// New improvements------------------------------------------------------------------------------
		if(Global.global_trCount > Global.observationWindow) {
			
			int _i = Global.global_trCount;		// _i ~ Sim.time() 
			int _W = Global.observationWindow;	// _W ~ time 
			
			HashSet<Integer> unq = new HashSet<Integer>(T);
			for(int _n = (_i - _W); n <= _i; n++) {
				unq.add(T.get(_n));
			}
			
			// Captures the number of total unique transaction for this observation window
			perfm.Unqlen.put((_i - _W), unq.size());
			
			// Calculate the impact of distributed transaction per transaction basis					
			double sum_of_span_by_period = 0.0;
			double sum_of_one_by_period = 0.0;
					
			Iterator<Integer> unq_itr = unq.iterator();
			while(unq_itr.hasNext()) {
				int unq_T = unq_itr.next();
				
				int span = perfm.Span.get(unq_T);
				double period = perfm.Period.get(unq_T);
				
				double span_by_period = span/period; // Frequency = 1/Period (f=1/t)
				double one_by_period = 1/period;	 // Frequency = 1/Period (f=1/t) per unit time (i.e. second)
				
				sum_of_span_by_period += span_by_period;
				sum_of_one_by_period += one_by_period;
			}
			
			double i_dt = (sum_of_span_by_period)/(Global.servers * sum_of_one_by_period);			
			perfm.I_Dt.put((_i - _W), i_dt);
			current_idt = i_dt;
			
			perfm.time.put((_i - _W), Sim.time());		
		}
		
		// Add a hyperedge to Workload Hypergraph
		wb.addHGraphEdge(cluster, tr);
		
		if(Global.workloadAware)
			if(Global.dataMigrationStrategy.equals("methodX"))
				wb.methodX.updateAssociation(cluster, tr, false);
		
		return tr;
	}
		
	public static void processTransaction(Transaction tr) {
		// Sleep for x milliseconds
    	try {    		
    		//TimeUnit.MILLISECONDS.sleep(tr.getTr_serverSpanCost());
    		Thread.sleep(tr.getTr_serverSpanCost());
    		
    		double response_time = (double)tr.getTr_serverSpanCost() + tr.getTr_waiting_time();    		
    		double avg_response = (response_time + tr.getTr_response_time()) / (int) (1/tr.getTr_period());
    		double round_avg_response = Math.round(avg_response * 100.0) / 100.0;
    		
    		total_response_time += round_avg_response;
    		
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
	public void execute(Database db, Cluster cluster, WorkloadBatch wb, Workload wrl) {
		Global.LOGGER.info("=============================================================================");
		Global.LOGGER.info("Streaming transactional workload ...");
		
		trDistribution = new EnumeratedIntegerDistribution(wrl.trTypes, wrl.trProbabilities);		
		trDistribution.reseedRandomGenerator(seed[Global.repeated_runs - 1]); 
		
		// Sword :: Create a new Workload Batch
		if(!Global.compressionBeforeSetup) {
			wb = new WorkloadBatch(Global.repeated_runs);
		
			if(Global.workloadAware)
				// MethodX :: initialisation
				if(Global.dataMigrationStrategy.equals("methodX"))
					wb.methodX.init(cluster);
		}
		
		// Start simulation
		WorkloadExecutor.simulate(db, cluster, wrl, wb, Global.simulationPeriod);
		
		// Show batch status in console	
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total "+Global.total_transactions+" transactions have processed "
				+ "by the Transaction Coordinator so far and of them "+Global.global_trSeq+" are unique.");
		
		Global.LOGGER.info("Total time: "+(Sim.time() / 3600)+" Hours");		
					
		// Statistic preparation, calculation, and reporting		
		collectStatistics(cluster, wb);
		cluster.show();		
		perfm.write();
	}	
	
	public static void collectStatistics(Cluster cluster, WorkloadBatch wb) {
		if(!WorkloadExecutor.noChangeRequired) { 
			cluster.updateLoad();

			wb.calculateDTI(cluster);		
			wb.calculateThroughput(Global.total_transactions);
			wb.calculateResponseTime(Global.total_transactions, total_response_time);
				
			Metric.collect(cluster, wb);			
			Metric.report();
			Metric.write();
			
			Global.nextCollection += 3600.0;
		}
	}
	
	public static void collectHourlyStatistics(Cluster cluster, WorkloadBatch wb) {
		
		if(Sim.time() >= Global.nextCollection) {
			
			Global.LOGGER.info("<-- Hourly Statistics -->");
			
			// Statistic preparation, calculation, and reporting			
			WorkloadExecutor.collectStatistics(cluster, wb);
			Global.nextCollection += 3600.0;
		}
	}
	
	public static void runRepartitioner(Cluster cluster, WorkloadBatch wb) {
		// Generate workload file
		boolean empty = false;
		try {
			empty = WorkloadBatchProcessor.generateWorkloadFile(cluster, wb);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		if(!empty) {
			
			int partitions = Global.partitions;
			Global.LOGGER.info("Starting partitioner to repartition the workload into "
					+partitions+" clusters ...");	
			
			// Perform hyper-graph/graph/compressed hyper-graph partitioning			 
			MinCut.runMinCut(wb, partitions, true);

			// Mapping cluster id to partition id
			Global.LOGGER.info("Applying data movement strategies ...");
			
			try {
				WorkloadBatchProcessor.processPartFile(cluster, wb, partitions);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
		} else {			
			// Update server-level load statistic and show
			cluster.updateLoad();								
			//cluster.show();
			
			WorkloadExecutor.noChangeRequired = true;
			
			Global.LOGGER.info("No changes are required !!!");
			Global.LOGGER.info("Repartitioning run aborting ...");
			Global.LOGGER.info("=======================================================================================================================");
		}
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
		Transaction tr = WorkloadExecutor.streamOneTransaction(this.db, this.cluster, this.wrl, this.wb);		
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

		// Checks when initial warm up state is reached
		if(Sim.time() >= 2*Global.observationWindow && WorkloadExecutor.initial) {
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Database finished warming up.");
			
			// Statistic preparation, calculation, and reporting
			wb.set_tr_nums(wb.hgr.getEdgeCount());
			WorkloadExecutor.threshold_idt = WorkloadExecutor.current_idt;						
			WorkloadExecutor.collectStatistics(cluster, wb);			
			WorkloadExecutor.initial = false;
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
		 * 		dataMigrationStrategy = Random/MC/MSM/Sword/MethodX		
		 * 
		 * 3. Incremental Repartitioning -- incremental repartitioning based on DT margin
		 * 		workloadAware = true
		 * 		incrementalRepartitioning = true
		 * 		enableTrClassification = true 
		 * 		workloadRepresentation = GR/HGR/CHG
		 * 		transactionClassificationStrategy = Basic/FD/FDFND
		 * 		dataMigrationStrategy = Random/MC/MSM/Sword/MethodX
		 *  
		 */
		
		if(!WorkloadExecutor.initial) {
			wb.set_tr_nums(wb.hgr.getEdgeCount());
			
			if(Global.workloadAware) {				
				if(Global.incrementalRepartitioning) { // 3. Incremental Repartitioning
					
					if(Sim.time() >= Global.nextCollection) {
					// Detecting increase in DTI from the threshold value
					//if(WorkloadExecutor.current_idt >= WorkloadExecutor.threshold_idt) {						
						
						++Global.repartitioningCycle;
						
						Global.LOGGER.info("-----------------------------------------------------------------------------");
						Global.LOGGER.info("Current simulation time: "+Sim.time()/3600+" hrs");
						Global.LOGGER.info("Significant increase in DTI has been detected !!!");			
						Global.LOGGER.info("Current DTI is "+WorkloadExecutor.current_idt+" which is above the threshold DTI of "+WorkloadExecutor.threshold_idt+".");
						Global.LOGGER.info("-----------------------------------------------------------------------------");
						Global.LOGGER.info("Total transactions processed so far: "+Global.total_transactions);
						Global.LOGGER.info("Total unique transactions processed so far: "+Global.global_trSeq);
						Global.LOGGER.info("Total unique transactions in the current observation window: "+wb.get_tr_nums());												
						Global.LOGGER.info("-----------------------------------------------------------------------------");
						Global.LOGGER.info("Starting database repartitioning ...");
						
						//Perform Data Stream Mining to find the transactions containing Distributed Semi-Frequent Closed Itemsets (tuples)
						if(Global.enableTrClassification) {
							Global.LOGGER.info("Identifying most frequently occurred transactions ...");
							
							switch(Global.trClassificationStrategy) {
								case "basic":
									TransactionClassifier.classifyMovableDTs(cluster, wb);
									break;
									
								case "fd":
									Global.LOGGER.info("Discarding transactions which are not frequent ...");
									TransactionClassifier.classifyMovableFD(cluster, wb);
									break;
									
								case "fdfnd":
									Global.LOGGER.info("Discarding transactions which are not frequent ...");
									TransactionClassifier.classifyMovableFDFND(cluster, wb);
									break;						
							}
						}
						
						Global.LOGGER.info("Total "+wb.hgr.getEdgeCount()+" transactions containing "
								+wb.hgr.getVertexCount()+" data objects have identified for repartitioning.");
						Global.LOGGER.info("-----------------------------------------------------------------------------");

						if(!Global.compressionBeforeSetup && !Global.dataMigrationStrategy.equals("methodX"))
							WorkloadExecutor.runRepartitioner(cluster, wb);
						
						// Perform data movement
						DataMovement.performDataMovement(cluster, wb, 
								Global.dataMigrationStrategy, Global.workloadRepresentation);

						Global.LOGGER.info("-----------------------------------------------------------------------------");						
						
						WorkloadExecutor.collectStatistics(cluster, wb);						
						WorkloadExecutor.changeDetected = false;
						
						//Global.LOGGER.info("DT baseline has been reset to "+WorkloadExecutor.threshold_idt);
						Global.LOGGER.info("=======================================================================================================================");
					}
					
				} else { // 2. Static Repartitioning
					
					if(Global.staticRun && WorkloadExecutor.changeDetected) {
						
						WorkloadExecutor.runRepartitioner(cluster, wb);
						WorkloadExecutor.collectStatistics(cluster, wb);
						Global.staticRun = false;
						
					} else if(!Global.staticRun)
						// Hourly statistic collection
						WorkloadExecutor.collectHourlyStatistics(cluster, wb);				
				}
				
			} else { // 1. Baseline
				
				// Hourly statistic collection
				WorkloadExecutor.collectHourlyStatistics(cluster, wb);
			}
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