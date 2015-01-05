package main.java.entry;

import java.io.IOException;
import java.util.Random;

import main.java.cluster.Cluster;
import main.java.db.TpccDatabase;
import main.java.metric.Metric;
import main.java.workload.TpccWorkload;
import main.java.workload.WorkloadBatch;
import main.java.workload.WorkloadExecutor;

import org.apache.commons.math3.random.RandomDataGenerator;

public class Main {
	//private static ReadConfig config = null;
	
	public static void main(String[] args) throws IOException {
		Global.rand = new Random();
		Global.rdg = new RandomDataGenerator();		
						
		Global.LOGGER.info("Simulating shared-nothing OLTP database cluster.");					
				
		ReadConfig.readArgs(args);
		
		switch(Global.wrl){
			case "tpcc":
				// Reading simulation aspects
				ReadConfig.readConfigFile("./sim.cnf");
				
				while(Global.repeated_runs != 0) {
	
					Global.LOGGER.info("=============================================================================");
					Global.LOGGER.info("Starting simulation for run "+Global.repeated_runs+" ...");									
					
					// Re-seed the Random Data Generator
					Global.rand.setSeed(Global.repeated_runs);
					Global.rdg.reSeed(Global.repeated_runs);
					
					TpccDatabase tpccDatabase = new TpccDatabase("tpcc");
					TpccWorkload tpcc = new TpccWorkload("/tpcc.cnf");
					Cluster dbCluster = new Cluster();
					WorkloadExecutor wrlExecutor = new WorkloadExecutor();
					Metric.init();
					
					// Read TPCC configurations
					tpcc.readConfig();
					
					// Estimate initial Table sizes
					tpccDatabase.estimateTableSize(tpcc);
					
					// Populate initial Database
					tpccDatabase.populate();
														
					// Assign Data popularity for Primary tables only
					tpcc.generateDataPopularity(tpccDatabase);
					
					// Warm up					
					dbCluster.warmup(tpccDatabase, tpcc);
					
					// Create a database cluster consisted of a set of physical servers and a consistent hash ring to store the physical data tuples
					WorkloadBatch wb = dbCluster.setup(tpccDatabase, tpcc);
					
					// Workload execution
					wrlExecutor.execute(tpccDatabase, dbCluster, wb, tpcc);									
					
					// Proceed for the next simulation run
					--Global.repeated_runs;																
				}
				
				Global.LOGGER.info("Simulation ended.");
				break;
		
			case "twitter":
				break;
		
			default:
				Global.LOGGER.info(Global.run_usage);
				Global.LOGGER.info(Global.abort);
				break;	
		} // end -- switch()		
	}
}