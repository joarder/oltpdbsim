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

package main.java.entry;

import java.io.IOException;
import java.util.Random;

import main.java.cluster.Cluster;
import main.java.db.Database;
import main.java.db.tpcc.TpccDatabase;
import main.java.db.twitter.TwitterDatabase;
import main.java.metric.Metric;
import main.java.workload.Workload;
import main.java.workload.WorkloadBatch;
import main.java.workload.WorkloadExecutor;
import main.java.workload.tpcc.TpccWorkload;
import main.java.workload.twitter.TwitterWorkload;

import org.apache.commons.math3.random.RandomDataGenerator;

public class Main {
	//private static ReadConfig config = null;
	
	public static void main(String[] args) throws IOException {
		Global.rand = new Random();
		Global.rdg = new RandomDataGenerator();		
						
		Global.LOGGER.info("Simulating shared-nothing OLTP database cluster.");					
				
		ReadConfig.readArgs(args);
		
		// Reading simulation aspects
		ReadConfig.readConfigFile("./sim.cnf");
		
		while(Global.repeated_runs != 0) {

			Global.LOGGER.info("=============================================================================");
			Global.LOGGER.info("Starting simulation for run "+Global.repeated_runs+" ...");									
			
			// Re-seed the Random Data Generator
			Global.rand.setSeed(Global.repeated_runs);
			Global.rdg.reSeed(Global.repeated_runs);
			
			// Database initialization and population
			Database db = null;
			Workload wrl = null;
			
			switch(Global.wrl){
				case "tpcc":					
					db = new TpccDatabase("tpcc");
					wrl = new TpccWorkload("/tpcc.cnf");
					break;
					
				case "twitter":
					db = new TwitterDatabase("twitter");
					wrl = new TwitterWorkload("/twitter.cnf");
					break;
					
				default:
					Global.LOGGER.info(Global.run_usage);
					Global.LOGGER.info(Global.abort);
					break;
			}
			
			// CLuster, Workload Executor, and Metric Collector initialization
			Cluster dbCluster = new Cluster();					
			WorkloadExecutor wrlExecutor = new WorkloadExecutor();					
			Metric.init(dbCluster);
			
			// Read TPCC configurations
			wrl.readConfig();
			
			// Populate initial database								
			db.populate(wrl);
			
			// Create a database cluster consisted of a set of physical servers and a consistent hash ring to store the physical data tuples
			WorkloadBatch wb = dbCluster.setup(db, wrl);
			
			// Workload execution
			wrlExecutor.execute(db, dbCluster, wb, wrl);									
			
			// Close Metric collector
			Metric.close();
			
			// Proceed for the next iterative simulation run
			Global.LOGGER.info("Simulation ended for iterative run-"+Global.repeated_runs+".");
			--Global.repeated_runs;																
		}		
				
		Global.LOGGER.info("Simulation ended.");		
	}
}