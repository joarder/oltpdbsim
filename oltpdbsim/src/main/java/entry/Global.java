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

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import main.java.repartition.DataStreamMining;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Global {

	// Global variables
	public static int global_index = 0;
	public static Map<Integer, Integer> global_index_map = new HashMap<Integer, Integer>();
	
	public static int global_trSeq = 0;
	public static int global_trCount = 0;
	public static int total_transactions = 0;
	public static int remove_count = 0;
	public static int global_tupleSeq = 0;
	public static int global_dataCount = 0;
	
	// Hyperedge and compressed hyperedge sequence number
	public static int hEdgeSeq = 0;
	public static int cHEdgeSeq = 0;
	
	// Random variable
	public static Random rand;
	// Random number generator
	public static RandomDataGenerator rdg;
	
	// Input parameters
	public static String wrl;
	public static String run;
	public static int repeated_runs;	
	
	// Data distribution related parameters
	public static String setup;
	public static int servers;
	public static int server_capacity;
	public static int partitions;
	public static long partition_capacity;
	public static int replicas;
	
	// OS name
	public static String OS = System.getProperty("os.name").toLowerCase();
	
	// Directory and extension names
	public static String dir_sep = "";
	public static String wrl_dir = "";
	public static String part_dir = "";
	public static String mining_dir = "";
	public static String metis_dir = "";
	public static String metric_dir = "";
	public static String metis_hgr_exec = "";
	public static String metis_gr_exec = "";
	public static String ext = "";
	public static String wrl_file_name = "workload.tr";
	public static String wrl_fixfile_name = "fixfile.tr";
	
	// Usage and abort message
	public static String wrl_usage = "Example usage: \"java -jar ddbmssim.jar wrl tpcc 1000 10\" or \"wrl twitter 2000 10\" to generate the desire workload";
	public static String run_usage = "Example usage: \"java -jar ddbmssim.jar run tpcc 1000 10 hgr\" or \"wrl twitter 2000 10 gr\" to run the simulation";
	public static String abort = "Aborting ...";
	
	// Workload execution
	public static double simulationPeriod;
	public static double warmupPeriod;
	
	public static double meanInterArrivalTime; 
    public static double meanServiceTime;
    
	public static double percentageChangeInWorkload;
	public static double adjustment;
	public static double expAvgWt; // Defines how far we need to look back while repeating transactions
	public static int observationWindow;
	public static int uniqueMaxFixed;
	
    // Workload mining
    public static int mining_serial = 0;
    public static DataStreamMining dsm;
    public static boolean streamCollection;
    public static int streamCollectorSizeFactor = 1;
    
    // ARHC and A-ARHC specific
    public static boolean adaptive;	// A-ARHC
    public static boolean associative; // ARHC
    
    // Simulation specific
    public static String simulation; // none/static/(gr/cgr/hgr/chg-basic/fd/fdfnd-random/mc/msm/sword)
    
    public static boolean workloadVariation;
    public static boolean workloadAware; // true/false
    public static boolean incrementalRepartitioning; // true/false
	public static boolean graphcutBasedRepartitioning; // true/false
    public static boolean enableTrClassification; // true/false
        
    public static boolean repartStatic;
    public static boolean repartHourly;
    public static boolean repartThreshold;
    
    public static String workloadRepresentation; // gr/cgr/hgr/chg
    public static String trClassificationStrategy; // basic/fd/fdfnd
    public static String dataMigrationStrategy; // random/mc/msm/sword
    
    public static boolean compressionEnabled;	// true/false
    public static boolean compressionBeforeSetup;
    public static double compressionRatio;    
    
    public static int repartitioningCycle = 0;
    public static double userDefinedIDtThreshold;
    
	// Logger
	public static Logger LOGGER = LoggerFactory.getLogger(Global.class);	

	public static int span_reduction;
	public static double idt_priority;
	public static double lb_priority;

	// For Sword
	public static int compressedVertices;
	public static boolean sword_initial = true;
	
	// For Analysis
	public static boolean analysis;
		
	public static String getRunDir() {
		return ("run"+Global.repeated_runs+Global.dir_sep);
	}
}