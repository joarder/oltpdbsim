package main.java.entry;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import main.java.utils.Utility;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class ReadConfig {
	
	public static void readArgs(String[] args) {
		Global.wrl = args[0];
		Global.LOGGER.info("Workload type: "+Global.wrl);
		
		Global.repeated_runs = Integer.parseInt(args[1]);
		Global.LOGGER.info("Targeted number of repeated runs: "+Global.repeated_runs);		
		
		// Setting directory name and structure according to OS
		if (Utility.isWindows()) {
			Global.dir_sep = "\\";
			
			Global.wrl_dir = System.getProperty("user.dir")+"\\workload"+"\\"+Global.wrl+"\\";
			Global.metis_dir = System.getProperty("user.dir")+"\\metis\\win\\";
			Global.part_dir = System.getProperty("user.dir")+"\\part\\";
			Global.mining_dir = System.getProperty("user.dir")+"\\mining\\";
			Global.metric_dir = System.getProperty("user.dir")+"\\metric\\";
			
			Global.metis_hgr_exec = "khmetis.exe";
			Global.metis_gr_exec = "gpmetis.exe";
			
		} else if (Utility.isUnix()) {
			Global.dir_sep = "/";
			
			Global.wrl_dir = System.getProperty("user.dir")+"/workload"+"/"+Global.wrl+"/";
			Global.metis_dir = System.getProperty("user.dir")+"/metis/unix/";
			Global.part_dir = System.getProperty("user.dir")+"/part/";
			Global.mining_dir = System.getProperty("user.dir")+"/mining/";
			Global.metric_dir = System.getProperty("user.dir")+"/metric/";
			
			Global.metis_hgr_exec = "khmetis";
			Global.metis_gr_exec = "gpmetis";
			
		} else {
			Global.LOGGER.error("Your OS is not supported !!");
		}
	}
	
	// Reads config.cnf
	public static void readConfigFile(String file_name) {
		FileInputStream config_file = null; 
	    AbstractFileConfiguration config_param = null;	    

	    Global.LOGGER.info("-----------------------------------------------------------------------------");
	    Global.LOGGER.info("Reading simulation aspects from file ...");
	    
	    try {
	    	// Access configuration file
	    	try {
	    		config_file = new FileInputStream(file_name);	    		
	    		Global.LOGGER.info("Simulation configuration file \"sim.cnf\" is found in the root directory and read.");
	    		
	    	} catch(IOException e){
	    		e.printStackTrace();
	    		System.out.println("Simulation configuration file \"sim.cnf\" is missing in the working directory !!");
	    		System.exit(0);
	    	}
	    	
		    // Load configuration parameters
	    	config_param = new PropertiesConfiguration();
			config_param.load(config_file);

			//Read the number of servers, partitions and replicas
			Global.servers = Integer.parseInt((String) config_param.getProperty("initial.servers"));
			Global.server_capacity = Integer.parseInt((String) config_param.getProperty("server.capacity"));
			Global.partitions = Integer.parseInt((String) config_param.getProperty("fixed.partitions"));
			Global.replicas = Integer.parseInt((String) config_param.getProperty("number.of.replicas"));			
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Initial number of servers: "+Global.servers);
			Global.LOGGER.info("Individual server's capacity: "+Global.server_capacity/1024+" GB");
			Global.LOGGER.info("Fixed number of partitions: "+Global.partitions);
			Global.LOGGER.info("Replication value: "+Global.replicas);			
			
			// Workload execution parameters --  will be used in Workload Executor
			Global.simulationPeriod = Integer.parseInt((String) config_param.getProperty("simulation.period"));
			Global.meanInterArrivalTime = Double.parseDouble((String) config_param.getProperty("inverse.of.mean.inter.arrival.time"));
			Global.meanServiceTime = Double.parseDouble((String) config_param.getProperty("inverse.of.mean.service.time"));
			Global.workloadChangeProbability = Double.parseDouble((String) config_param.getProperty("workload.change.probability"));			
			Global.percentageChangeDt = Double.parseDouble((String) config_param.getProperty("threshold.change.in.dt"));			
			Global.dynamicDtMargin = Boolean.parseBoolean((String) config_param.getProperty("dynamic.dt.margin"));
			Global.initialDetectionTime = Integer.parseInt((String) config_param.getProperty("initial.detection.time"));
			
			Global.oldTransactionTimestamp = Integer.parseInt((String) config_param.getProperty("old.transaction.timestamp"));
			Global.transactionExpiration = Boolean.parseBoolean((String) config_param.getProperty("enable.transaction.expiration"));
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Simulation Period: "+Global.simulationPeriod);
			Global.LOGGER.info("Probability of Transaction birth and death: "+Global.workloadChangeProbability);
	    	Global.LOGGER.info("Mean inter Transaction arrival time: "+Global.meanInterArrivalTime);
	    	Global.LOGGER.info("Mean Transaction service time: "+Global.meanServiceTime);			
						
			// Read configuration parameters			
			Global.simulation = (String) config_param.getProperty("simulation.name");
			Global.workloadAware = Boolean.parseBoolean((String) config_param.getProperty("workload.aware"));
			Global.workloadRepresentation = (String) config_param.getProperty("workload.representation");
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Simulation name: "+Global.simulation);
			Global.LOGGER.info("Workload aware: "+Global.workloadAware);
			Global.LOGGER.info("Workload representation: "+Global.workloadRepresentation);
			
			if(Global.workloadAware) {
				
				Global.incrementalRepartitioning = Boolean.parseBoolean((String) config_param.getProperty("incremental.repartitioning"));
				Global.enableTrClassification = Boolean.parseBoolean((String) config_param.getProperty("transaction.classification"));
												
				Global.trClassificationStrategy = (String) config_param.getProperty("transaction.classification.strategy");
				Global.dataMigrationStrategy = (String) config_param.getProperty("data.migration.strategy");
				
				
				Global.compressionEnabled = Boolean.parseBoolean((String) config_param.getProperty("compression.enabled"));
				Global.compressionBeforeSetup = Boolean.parseBoolean((String) config_param.getProperty("compression.before.setup"));
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");				
				Global.LOGGER.info("Incremental repartitioning: "+Global.incrementalRepartitioning);
				Global.LOGGER.info("Transaction classification: "+Global.enableTrClassification);
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");				
				Global.LOGGER.info("Transaction classification strategy: "+Global.trClassificationStrategy);
				Global.LOGGER.info("Data movement strategy: "+Global.dataMigrationStrategy);
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");
				Global.LOGGER.info("Workload compression enabled: "+Global.compressionEnabled);
				Global.LOGGER.info("Compression before setup enabled: "+Global.compressionBeforeSetup);
	
				if(Global.compressionEnabled) {
					Global.compressionRatio = Double.parseDouble((String) config_param.getProperty("compression.ratio"));
					Global.LOGGER.info("Compression ratio: "+Global.compressionRatio);
				}
			}
			
	    } catch (ConfigurationException e) {
	    	Global.LOGGER.error("Failed to read the configurations from sim.cnf file !!", e);
		} finally {
			if(config_file != null) {
				try {
					config_file.close();			
				} catch (IOException e) {
					Global.LOGGER.error("Failed to close the sim.cnf file !!", e);
				}			
			}
		}
	}
	
	// Reads global.cnf
	public void readGlobal(String file_name) {
		BufferedReader config_file = null; 
	    AbstractFileConfiguration parameters = null;
	    
	    Global.LOGGER.info("-----------------------------------------------------------------------------");
	    Global.LOGGER.info("Reading global configurations from file ...");
	    
	    try {
	    	config_file = new BufferedReader(new InputStreamReader(this.getClass().getResourceAsStream(file_name)));
  	
			Global.LOGGER.info("Global Configuration file "+file_name+" is found under src/main/resources and read."); 
	    	
		    //Load configuration parameters
	    	parameters = new PropertiesConfiguration();
			parameters.load(config_file);
					 
			//Read the number of servers, partitions and replicas
			Global.servers = parameters.getInt("initial.servers");
			Global.server_capacity = parameters.getInt("server.capacity");
			Global.partitions = parameters.getInt("fixed.partitions");
			Global.replicas = parameters.getInt("number.of.replicas");			
			
			Global.LOGGER.info("Initial number of servers: "+Global.servers);
			Global.LOGGER.info("Individual server's capacity: "+Global.server_capacity/1024+" GB");
			Global.LOGGER.info("Fixed number of partitions: "+Global.partitions);
			Global.LOGGER.info("Replication value: "+Global.replicas);
			
			// Workload execution parameters --  will be used in Workload Executor
			Global.simulationPeriod = parameters.getInt("simulation.period");
			Global.meanInterArrivalTime = parameters.getDouble("mean.inter.arrival.time");
			Global.meanServiceTime = parameters.getDouble("mean.service.time");
			
			Global.LOGGER.info("Simulation Period: "+Global.simulationPeriod);
	    	Global.LOGGER.info("Mean inter Transaction arrival time: "+Global.meanInterArrivalTime);
	    	Global.LOGGER.info("Mean Transaction service time: "+Global.meanServiceTime);
			
		} catch (ConfigurationException e) {
			Global.LOGGER.error("Failed to read the configurations from global.cnf file !!", e);
		} finally {
			if(config_file != null) {
				try {
					config_file.close();			
				} catch (IOException e) {
					Global.LOGGER.error("Failed to close the global.cnf file !!", e);
				}			
			}
		}
	}
}