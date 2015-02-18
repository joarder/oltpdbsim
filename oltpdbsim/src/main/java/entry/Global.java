package main.java.entry;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.random.RandomDataGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Global implements java.io.Serializable {

	private static final long serialVersionUID = -6044446681907434051L;
	
	// Global variables
	public static int global_index = 0;
	public static Map<Integer, Integer> global_index_map = new HashMap<Integer, Integer>();
	
	public static int global_trSeq = 0;
	public static int global_trCount = 0;
	public static int total_transactions = 0;
	public static int remove_count = 0;
	public static int global_tupleSeq = 0;
	public static int global_dataCount = 0;
	
	// Graph, Hypergraph
	public static int edgeSeq = 0;
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
	public static int uniqueMax;	
	public static double nextCollection;
	
    // Workload mining
    public static int mining_serial = 0;
    
    // Simulation specific
    public static String simulation; // none/static/(gr/cgr/hgr/chg-basic/fd/fdfnd-random/mc/msm/sword)
    
    public static boolean workloadAware; // true/false
    public static boolean incrementalRepartitioning; // true/false
    public static boolean enableTrClassification;
    public static boolean staticRun = true;
    
    public static String workloadRepresentation; // gr/cgr/hgr/chg
    public static boolean hourlyRepartitioning;
    public static String trClassificationStrategy; // basic/fd/fdfnd
    public static String dataMigrationStrategy; // random/mc/msm/sword
    
    public static boolean compressionEnabled;	// true/false
    public static boolean compressionBeforeSetup;
    public static double compressionRatio;    
    
    public static int repartitioningCycle = 0;
    public static double percentageIDtThresholdInc;
    
    // TPC-C specific
    public static final int[] tpccTrTypes = {1, 2, 3, 4, 5};
    public static final double[] tpccTrProbabilities = {0.45, 0.43, 0.04, 0.04, 0.04};
    public static int[] tpccLineNumbers = new int[5];
    
	// Logger
	public static Logger LOGGER = LoggerFactory.getLogger(Global.class);

	public static int virtualDataNodes;	
	
	// For methodX whether prioritise dt(alpha) minimisation or load balance (1-alpha)
	public static double priority;
	
	public static String getRunDir() {
		return ("run"+Global.repeated_runs+Global.dir_sep);
	}
}