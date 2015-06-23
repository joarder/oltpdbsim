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
import java.util.Collections;
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
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.db.Database;
import main.java.entry.Global;
import main.java.metric.Metric;
import main.java.metric.PerfMetric;
import main.java.repartition.Analysis;
import main.java.repartition.DataMigration;
import main.java.repartition.DataStreamMining;
import main.java.repartition.MinCut;
import main.java.repartition.RBPTA;
import main.java.repartition.SimpleTr;
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
	static boolean warmingUp = true;
    static boolean changeDetected = false;
    static boolean noChangeIsRequired = false;
    static boolean repartitioningCoolingOff = false;
    public static boolean isAdaptive = false;
    
	static int uniqueMax;	
	static double nextStatCollection = Global.observationWindow;
	static double nextHourlyRepartition = Global.warmupPeriod;
	
	static boolean varyWorkload = false;
	static double nextWorkloadVariation = Global.warmupPeriod + 2*Global.observationWindow;
	
// New improvements------------------------------------------------------------------------------
	static ArrayList<Integer> T = new ArrayList<Integer>();
	static Map<Integer, Double> Time = new HashMap<Integer, Double>();
	static PerfMetric perfm = new PerfMetric();
	static int _i = 0;
	static int _W = 0;
	
	static double currentIDt = 0.0;
	static double RepartitioningCoolingOffPeriod = Global.observationWindow;
	
	// Seed
	static long[] seed = new long[10];
	
	// New (January 07, 2015)
	static Map<Integer, Integer> idx = new HashMap<Integer, Integer>();	
	static Set<Integer> toBeRemoved = new TreeSet<Integer>();
	
    public WorkloadExecutor() {
        
        MRG32k3a rand = new MRG32k3a();                
        for(int i = 0; i < 4; i++)
        	seed[i] = 1; //(long) Global.repeated_runs + i;

        rand.setSeed(seed);
        
        genArr = new ExponentialGen(rand, Global.meanInterArrivalTime); // mean inter arrival rate = 1/lambda        
		genServ = new ExponentialGen(rand, Global.meanServiceTime); // mean service time = 1/mu
		
		uniqueMax = Global.uniqueMaxFixed - (int)(Global.observationWindow * Global.percentageChangeInWorkload * Global.adjustment);
    }
	
	public static Transaction streamOneTransaction(Database db, Cluster cluster, 
			Workload wrl, WorkloadBatch wb) {

		if(varyWorkload)
			varyWorkload();
		
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
			
			// Add the incident transaction id
			wb.addIncidentTr(cluster, trDataSet, Global.global_trSeq);
			
			// Add the newly created Transaction in the Workload Transaction map	
			wb.getTrMap().get(type).put(tr.getTr_id(), tr);
			
// New improvements------------------------------------------------------------------------------
			double initial_period = (double) WorkloadExecutor.uniqueMax;
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
		
		// Update Idt
		tr.calculateIdt();
		
		if(perfm.Span.containsKey(tr.getTr_id()))
			perfm.Span.remove(tr.getTr_id());
		
		perfm.Span.put(tr.getTr_id(), tr.getTr_serverSpanCost());
		
		// Create an index entry for each newly created Transaction		
		idx.put(tr.getTr_id(), Global.global_trCount);
		T.add(tr.getTr_id());		
		
// New improvements------------------------------------------------------------------------------
		if(Global.global_trCount > Global.observationWindow) {
			
			_i = Global.global_trCount;		// _i ~ Sim.time() 
			_W = Global.observationWindow;	// _W ~ time 
			
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
				
				double span_by_period = span/period; // Frequency = 1/Period (f=1/t) per unit time (i.e. 1 second)
				double one_by_period = 1/period;	 // Frequency = 1/Period (f=1/t) per unit time (i.e. 1 second)
				
				sum_of_span_by_period += span_by_period;
				sum_of_one_by_period += one_by_period;
			}
			
			double i_dt = (sum_of_span_by_period)/(Global.servers * sum_of_one_by_period);			
			perfm.I_Dt.put((_i - _W), i_dt);
			
			if(Double.isNaN(i_dt))
				currentIDt = 0;
			else
				currentIDt = i_dt;
			
			// Resetting repartitioning cooling off period
			if(WorkloadExecutor.repartitioningCoolingOff && Sim.time() >= WorkloadExecutor.RepartitioningCoolingOffPeriod) {
				
				WorkloadExecutor.repartitioningCoolingOff = false;				
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");
				Global.LOGGER.info("Simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
				Global.LOGGER.info("Repartitioning cooling off period ends.");
				Global.LOGGER.info("System will now check whether another repartitioning is required at this moment.");
				Global.LOGGER.info("Current IDt: "+currentIDt);
				Global.LOGGER.info("User defined IDt threshold: "+Global.userDefinedIDtThreshold);
				
				if(currentIDt < Global.userDefinedIDtThreshold) {
					Global.LOGGER.info("Repartitioning is not required at this moment.");
					Global.LOGGER.info("Continuing transaction processing ...");
					
					// Adaptive ARHC
					if(Global.adaptive) {
						WorkloadExecutor.isAdaptive = false;
						
						wb.set_intra_dmv(DataMigration.intra_server_dmgr);
						wb.set_inter_dmv(DataMigration.inter_server_dmgr);
						
						Global.LOGGER.info("-----------------------------------------------------------------------------");												
						WorkloadExecutor.collectStatistics(cluster, wb);
					}					
				}
			}
			
			perfm.time.put((_i - _W), Sim.time());		
		}
		
		// Add a hyperedge to Workload Hypergraph
		wb.addHGraphEdge(cluster, tr);
		
		// Collect transaction stream if data stream mining is enabled
		if(Global.streamCollection)
			Global.dsm.collectStream(cluster, tr);
		
		return tr;
	}
		
	public static void processTransaction(Transaction tr) {
		// Sleep for x milliseconds
    	try {
    		//Thread.sleep(tr.getTr_serverSpanCost()); // Actually no need
    		tr.setProcessed(true);
    		
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
	
	public void execute(Database db, Cluster cluster, WorkloadBatch wb, Workload wrl) {
		Global.LOGGER.info("=============================================================================");
		Global.LOGGER.info("Warming up the database ...");
		Global.LOGGER.info("Streaming transactional workloads ...");
		
		trDistribution = new EnumeratedIntegerDistribution(wrl.trTypes, wrl.trProbabilities);		
		trDistribution.reseedRandomGenerator(seed[Global.repeated_runs - 1]); 
				
		wb = new WorkloadBatch(Global.repeated_runs);
		
		// RBPTA specific -- will be removed
		if(Global.dataMigrationStrategy.equals("rbpta"))
			RBPTA.init(cluster);
		
		// Start simulation
		WorkloadExecutor.simulate(db, cluster, wrl, wb, Global.simulationPeriod);
		
		// Show batch status in console	
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total "+Global.total_transactions+" transactions have processed "
				+ "by the Transaction Coordinator so far and of them "+Global.global_trSeq+" are unique.");
		
		Global.LOGGER.info("Total time: "+(Sim.time()/Global.observationWindow)+" hrs");		
					
		// Statistic preparation, calculation, and reporting		
		collectStatistics(cluster, wb);
		cluster.show();
		
		perfm.write();
	}	

	// On-repartition Statistics Collection
	public static void collectStatistics(Cluster cluster, WorkloadBatch wb) {
		if(!WorkloadExecutor.noChangeIsRequired) { 
			cluster.updateLoad();

			wb.setIdt(currentIDt);
			wb.calculateDTI(cluster);		
			wb.calculateThroughput(Global.total_transactions);
				
			Metric.collect(cluster, wb);			
			Metric.report();
			Metric.write();						
			
			// Reset each time	
			wb.set_intra_dmv(0);
			wb.set_inter_dmv(0);
			
			for(Server s : cluster.getServers()) { 
				s.setServer_inflow(0);
				s.setServer_outflow(0);
			}
			
			for(Partition p : cluster.getPartitions()) {
				p.setPartition_inflow(0);
				p.setPartition_outflow(0);
			}						
			
			Global.LOGGER.info("=======================================================================================================================");
		}
	}
	
	// Hourly Statistics Collection
	public static void collectHourlyStatistics(Cluster cluster, WorkloadBatch wb) {	
		
		if(Sim.time() >= WorkloadExecutor.nextStatCollection) {				
			Global.LOGGER.info("<-- Hourly Statistics -->");			
			
			if(Global.analysis && !WorkloadExecutor.warmingUp)
				Analysis.analyse(wb, perfm);
			
			WorkloadExecutor.collectStatistics(cluster, wb);
			
			// Always schedule an hourly collection
			WorkloadExecutor.nextStatCollection += Global.observationWindow;
		}
	}
	
	public static void runRepartitioner(Cluster cluster, WorkloadBatch wb) {
		// Generate workload file
		boolean empty = false;
		
		if(!Global.associative) {
			try {
				empty = WorkloadBatchProcessor.generateWorkloadFile(cluster, wb);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		if(!empty) {
			
			int k_clusters = 0;
			
			if(Global.associative)
				k_clusters = Global.servers;
			else
				k_clusters = Global.partitions;
			
			Global.LOGGER.info("Starting partitioner to repartition the workload into "+k_clusters+" clusters ...");	
			
			// Perform k-way graph/hypergraph/compressed hypergraph clustering			 
			MinCut.runMinCut(wb, k_clusters, true);

			// Mapping cluster id to partition id
			Global.LOGGER.info("Applying data migration strategy ...");
			
			try {
				WorkloadBatchProcessor.processPartFile(cluster, wb, k_clusters);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			WorkloadExecutor.noChangeIsRequired = false;
			
		} else {			
			// Update server-level load statistic and show
			cluster.updateLoad();								
			//cluster.show();
			
			WorkloadExecutor.noChangeIsRequired = true;
			
			Global.LOGGER.info("No changes are required !!!");
			Global.LOGGER.info("Repartitioning run aborting ...");
			Global.LOGGER.info("=======================================================================================================================");
		}
	}
	
	public static void startRepartitioning(Cluster cluster, WorkloadBatch wb) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total transactions processed so far: "+Global.total_transactions);
		Global.LOGGER.info("Total unique transactions processed so far: "+Global.global_trSeq);
		Global.LOGGER.info("Total unique transactions in the current observation window: "+wb.get_tr_nums());												
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Starting database repartitioning ...");
		
		if(!Global.dataMigrationStrategy.equals("rbpta")) {						
			//Perform Data Stream Mining to find the transactions containing Distributed Semi-Frequent Closed Itemsets (tuples)
			if(Global.enableTrClassification) {
				Global.LOGGER.info("Identifying most frequently occurred transactions ...");
				
				switch(Global.trClassificationStrategy) {
					case "basic":
						TransactionClassifier.classifyMovableNonDTs(cluster, wb);
						break;
						
					case "fd":
						Global.LOGGER.info("Discarding transactions which are not frequent ...");						
						TransactionClassifier.classifyFD(cluster, wb);
						break;
						
					case "fdfnd":
						Global.LOGGER.info("Discarding transactions which are not frequent ...");
						TransactionClassifier.classifyFDFND(cluster, wb);
						break;
						
					case "fcimining":	
						Global.LOGGER.info("Mining frequent tuple sets from transactional streams ...");						
						Global.dsm.performDSM(cluster, wb);
						break;
				}
			}
			
			if(!Global.associative) {
				Global.LOGGER.info("Total "+wb.hgr.getEdgeCount()+" transactions containing "
						+wb.hgr.getVertexCount()+" data tuples have identified for repartitioning.");
				Global.LOGGER.info("-----------------------------------------------------------------------------");
		
				if(Global.graphcutBasedRepartitioning && Global.sword_initial)
					WorkloadExecutor.runRepartitioner(cluster, wb);
								
				// Perform data migrations
				DataMigration.performDataMigration(cluster, wb);
			}
		}
	}
	
	static boolean beginning = true;
	static boolean firstArrival1 = false;
	static boolean firstArrival2 = false;
	static boolean incline = false;
	
	private static void varyWorkload() {
		if(Sim.time() >= nextWorkloadVariation) {			
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
			Global.LOGGER.info("Current workload variation: "+Global.percentageChangeInWorkload);
			Global.LOGGER.info("Current variation adjustment: "+Global.adjustment);
			Global.LOGGER.info("Varying workload ...");
			
			if(Global.percentageChangeInWorkload > 0.01 &&  Global.percentageChangeInWorkload < 0.25) {
				
				if(incline) {
					double val = Global.percentageChangeInWorkload + 0.05;
					Global.percentageChangeInWorkload = (double)Math.round(val * 100)/100;
				
				} else {
					double val = Global.percentageChangeInWorkload - 0.05;
					Global.percentageChangeInWorkload = (double)Math.round(val * 100)/100;
					
					if(Global.percentageChangeInWorkload == 0)
						Global.percentageChangeInWorkload = 0.01;
				}
				
			} else if(Global.percentageChangeInWorkload == 0.25) {
				if(!firstArrival2) {					
					firstArrival2 = true;
					
				} else {
					double val = Global.percentageChangeInWorkload - 0.05;
					Global.percentageChangeInWorkload = (double)Math.round(val * 100)/100;
					
					firstArrival2 = false;
					incline = false;
				}
				
			} else if(Global.percentageChangeInWorkload == 0.01) {				
				if(!firstArrival1) {					
					firstArrival1 = true;
					
					if(beginning) {
						Global.percentageChangeInWorkload = 0.05;
						
						incline = true;
						beginning = false;
					}
					
				} else {
					Global.percentageChangeInWorkload = 0.05;
					
					firstArrival1 = false;
					incline = true;
				}
			}
			
			Global.LOGGER.info("New workload variation: "+Global.percentageChangeInWorkload);
			
			Global.adjustment = wrlVarAdjustment.get(Global.percentageChangeInWorkload);
			Global.LOGGER.info("New variation adjustment: "+Global.adjustment);
			
			uniqueMax = Global.uniqueMaxFixed - (int)(Global.observationWindow * Global.percentageChangeInWorkload * Global.adjustment);
			
			nextWorkloadVariation += 2*Global.observationWindow;			
			Global.LOGGER.info("Workload vairation will occur again at "+(nextWorkloadVariation/Global.observationWindow)+" hrs.");			
		}		
	}
	
	private static final Map<Double, Double> wrlVarAdjustment;
	static {
		Map<Double, Double> map = new HashMap<Double, Double>();
		
		map.put(0.01, 0.1);
		map.put(0.05, 0.2);
		map.put(0.10, 0.4);
		map.put(0.15, 0.6);
		map.put(0.20, 0.8);
		map.put(0.25, 1.0);
		
		wrlVarAdjustment = Collections.unmodifiableMap(map);
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

		// Hourly statistic collection while the database is warming up
		if(WorkloadExecutor.warmingUp) {
			if(!(Sim.time() >= Global.warmupPeriod))
				WorkloadExecutor.collectHourlyStatistics(cluster, wb);
		}
		
		// Checks when initial warm up state is reached
		if(Sim.time() >= Global.warmupPeriod && WorkloadExecutor.warmingUp) {
			WorkloadExecutor.collectHourlyStatistics(cluster, wb);
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");			
			Global.LOGGER.info("Database finished warming up.");
			Global.LOGGER.info("Simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
			
			WorkloadExecutor.warmingUp = false;
			
			if(Global.workloadVariation)
				WorkloadExecutor.varyWorkload = true;
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
		 * 		dataMigrationStrategy = Random/MC/MSM/Sword/Association
		 * 
		 * 3. Incremental Repartitioning -- incremental repartitioning based on DT margin
		 * 		workloadAware = true
		 * 		incrementalRepartitioning = true
		 * 		enableTrClassification = true 
		 * 		workloadRepresentation = GR/HGR/CHG
		 * 		transactionClassificationStrategy = Basic/FD/FDFND
		 * 		dataMigrationStrategy = Random/MC/MSM/Sword/Association
		 *  
		 */
		
		if(!WorkloadExecutor.warmingUp) {
			wb.set_tr_nums(wb.hgr.getEdgeCount());
			
			if(Global.workloadAware) {
				
				if(Global.incrementalRepartitioning && !Global.sword_initial) { // 3. Incremental Repartitioning
					
					if(isRepartRequired()) { // Checks for both Hourly and Threshold-based repartitioning						
						++Global.repartitioningCycle;
						
						if(Global.repartThreshold) {
							Global.LOGGER.info("-----------------------------------------------------------------------------");
							Global.LOGGER.info("Current simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
							Global.LOGGER.info("Significant increase in DTI has been detected !!!");			
							Global.LOGGER.info("Current DTI is "+WorkloadExecutor.currentIDt+" which is above the user defined threshold DTI of "+Global.userDefinedIDtThreshold+".");
							Global.LOGGER.info("Repartitioning will start now ...");
							
						} else if(Global.repartHourly){
							Global.LOGGER.info("-----------------------------------------------------------------------------");
							Global.LOGGER.info("Current simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
							Global.LOGGER.info("Hourly repartitioning will start now ...");
						}
						
						// Repartition the database in an incremental manner
						if(Global.associative && Global.adaptive) {
							wb.set_intra_dmv(DataMigration.intra_server_dmgr);
							wb.set_inter_dmv(DataMigration.inter_server_dmgr);
							
							Global.LOGGER.info("-----------------------------------------------------------------------------");												
							WorkloadExecutor.collectStatistics(cluster, wb);
							
							Global.LOGGER.info("-----------------------------------------------------------------------------");
							Global.LOGGER.info("Starting adaptive data stream mining ...");
							Global.LOGGER.info("Identifying most frequently occurred transactions ...");
							
							Global.dsm.performDSM(cluster, wb);
							
							WorkloadExecutor.isAdaptive = true;
							
						} else {
							WorkloadExecutor.startRepartitioning(cluster, wb);
						}
						
						if(!Global.adaptive) {
							Global.LOGGER.info("-----------------------------------------------------------------------------");												
							WorkloadExecutor.collectStatistics(cluster, wb);
						}
						
						if(Global.repartThreshold) {
							WorkloadExecutor.repartitioningCoolingOff = true;
							WorkloadExecutor.RepartitioningCoolingOffPeriod = Sim.time() + Global.observationWindow; // Wait W period to reset the threshold

							Global.LOGGER.info("-----------------------------------------------------------------------------");
							Global.LOGGER.info("Repartitioning cooling off has started. No further repartitioning will take place within the next hour.");
							Global.LOGGER.info("Repartitioning cooling off period will end at "+WorkloadExecutor.RepartitioningCoolingOffPeriod/(double)Global.observationWindow+" hrs.");
							
							if(Global.adaptive)
								Global.LOGGER.info("Starting adaptive data redistribution while processing each transactions ...");
						}						
					}					
					
				} else { // 2. Static Repartitioning

					if(Global.repartStatic) {
						Global.LOGGER.info("-----------------------------------------------------------------------------");
						Global.LOGGER.info("Current simulation time: "+Sim.time()/(double)Global.observationWindow+" hrs");
						Global.LOGGER.info("<-- Static Repartioning -->");
						Global.LOGGER.info("Start repartioning the database for a single time ...");
						
						// Repartition the database for a single time
						WorkloadExecutor.startRepartitioning(cluster, wb);
						
						// Statistics collection
						WorkloadExecutor.collectStatistics(cluster, wb);
						
						// Prevents future repartitioning
						Global.repartStatic = false;
						
						// Sword single repartitioning is completed
						if(Global.compressionBeforeSetup)
							Global.sword_initial = false;
						
					} else { 						
						// Hourly statistic collection
						WorkloadExecutor.collectHourlyStatistics(cluster, wb);
					}
				}
				
			} else { // 1. Baseline
				// Hourly statistic collection				
				WorkloadExecutor.collectHourlyStatistics(cluster, wb);
			}						
			
			// Adaptive ARHC
			if(Global.associative && Global.adaptive && WorkloadExecutor.isAdaptive) {
				// Redistribute transactional tuples in an adaptive manner using DSM/ARHC
				SimpleTr t = DataStreamMining.prepare(cluster, wb, wb.hgr.getHEdge(tr.getTr_id()));
				t.populateAssociationList(cluster, wb, DataStreamMining.fci_clusters);
				
				if(t.isAssociated)
					DataStreamMining.processTransaction(cluster, wb, t, t.migrationPlanList.get(0));
			}
		}
		
		if(Global.dataMigrationStrategy.equals("rbpta"))
			RBPTA.updateAssociation(cluster, tr);
	}
	
	private boolean isRepartRequired() {
		
		if(Global.repartHourly) {
			
			if(Sim.time() >= WorkloadExecutor.nextHourlyRepartition) {				
				WorkloadExecutor.nextHourlyRepartition += Global.observationWindow;
				return true;
			}
				
		} else if(Global.repartThreshold) {			
			
			if(WorkloadExecutor.currentIDt > Global.userDefinedIDtThreshold 
					&& !WorkloadExecutor.repartitioningCoolingOff)					
				return true;
			
		} else 
			Global.LOGGER.error("Check the simulation configuration file 'sim.cnf' for 'repartitioning.strategy' the value of which can either be 'hourly' or 'threshodl'.");
			
		
		return false;
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