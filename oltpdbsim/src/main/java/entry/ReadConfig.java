package main.java.entry;

import java.io.FileInputStream;
import java.io.IOException;

import main.java.repartition.DataStreamMining;
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
			Global.setup = (String) config_param.getProperty("setup");
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
			Global.simulationPeriod = Double.parseDouble((String) config_param.getProperty("simulation.period"));
			Global.warmupPeriod = Double.parseDouble((String) config_param.getProperty("warmup.period"));			
			
			Global.meanInterArrivalTime = Double.parseDouble((String) config_param.getProperty("inverse.of.mean.inter.arrival.time"));
			Global.meanServiceTime = Double.parseDouble((String) config_param.getProperty("inverse.of.mean.service.time"));
			
			Global.percentageChangeInWorkload = Double.parseDouble((String) config_param.getProperty("percentage.change.in.workload"));
			Global.adjustment = Double.parseDouble((String) config_param.getProperty("adjustment"));			
			Global.observationWindow = Integer.parseInt((String) config_param.getProperty("observation.window.size"));
			Global.uniqueMaxFixed = Integer.parseInt((String) config_param.getProperty("unique.max.fixed"));
			Global.expAvgWt = Double.parseDouble((String) config_param.getProperty("exponential.Avg.Weight"));
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Simulation name: "+Global.simulation);
			Global.LOGGER.info("Simulation Period: "+(Global.simulationPeriod/Global.observationWindow)+" hrs");
			Global.LOGGER.info("Warmup time: "+(Global.warmupPeriod/Global.observationWindow)+" hrs");
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Probability of Transaction birth and death: "+Global.percentageChangeInWorkload);
	    	Global.LOGGER.info("Mean inter Transaction arrival time: "+Global.meanInterArrivalTime);
	    	Global.LOGGER.info("Mean Transaction service time: "+Global.meanServiceTime);			
						
			// Read configuration parameters			
			Global.simulation = (String) config_param.getProperty("simulation.name");
			Global.analysis = Boolean.parseBoolean((String) config_param.getProperty("analysis"));
			Global.workloadVariation = Boolean.parseBoolean((String) config_param.getProperty("workload.variation"));
			Global.workloadAware = Boolean.parseBoolean((String) config_param.getProperty("workload.aware"));
			Global.workloadRepresentation = (String) config_param.getProperty("workload.representation");
			
			Global.LOGGER.info("-----------------------------------------------------------------------------");Global.LOGGER.info("Workload aware: "+Global.workloadAware);
			Global.LOGGER.info("Workload representation: "+Global.workloadRepresentation);
			
			if(Global.workloadAware) {				
				
				Global.incrementalRepartitioning = Boolean.parseBoolean((String) config_param.getProperty("incremental.repartitioning"));
				Global.graphcutBasedRepartitioning = Boolean.parseBoolean((String) config_param.getProperty("graphcutbased.repartitioning"));
				
				Global.repartStatic = Boolean.parseBoolean((String) config_param.getProperty("static.repartitioning"));
				Global.repartHourly = Boolean.parseBoolean((String) config_param.getProperty("hourly.repartitioning"));
				Global.repartThreshold = Boolean.parseBoolean((String) config_param.getProperty("threshold.repartitioning"));
				
				if(Global.repartStatic)
					Global.singleRun = true;
								
				Global.streamCollection = Boolean.parseBoolean((String) config_param.getProperty("stream.collection"));								
				Global.streamCollectorSizeFactor = Integer.parseInt((String) config_param.getProperty("stream.collector.size.factor"));
				
				if(Global.streamCollection) {
					Global.dsm = new DataStreamMining();
					Global.adaptive = Boolean.parseBoolean((String) config_param.getProperty("adaptive"));
				}
				
				Global.enableTrClassification = Boolean.parseBoolean((String) config_param.getProperty("transaction.classification"));
				Global.trClassificationStrategy = (String) config_param.getProperty("transaction.classification.strategy");
				
				Global.dataMigrationStrategy = (String) config_param.getProperty("data.migration.strategy");
								
				Global.compressionEnabled = Boolean.parseBoolean((String) config_param.getProperty("compression.enabled"));
				Global.compressionBeforeSetup = Boolean.parseBoolean((String) config_param.getProperty("compression.before.setup"));
				
				if(Global.incrementalRepartitioning) {
					Global.userDefinedIDtThreshold = Double.parseDouble((String) config_param.getProperty("idt.threshold"));					
					Global.span_reduction = Integer.parseInt((String) config_param.getProperty("span.reduction"));
					Global.idt_priority = Double.parseDouble((String) config_param.getProperty("idt.priority"));
						
					if(Global.idt_priority < 0 || Global.idt_priority > 1) {
						Global.LOGGER.error("Wrong value set for the Idt priority !!");
						Global.idt_priority = 0.5;
					}
					
					Global.lb_priority = 1 - Global.idt_priority;
				}
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");
				Global.LOGGER.info("Incremental repartitioning: "+Global.incrementalRepartitioning);
				Global.LOGGER.info("Static repartitioning: "+Global.repartStatic);
				Global.LOGGER.info("Hourly repartitioning: "+Global.repartHourly);
				Global.LOGGER.info("Threshold-based repartitioning: "+Global.repartThreshold);
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");
				Global.LOGGER.info("Transaction classification: "+Global.enableTrClassification);
				Global.LOGGER.info("Transaction classification strategy: "+Global.trClassificationStrategy);
				
				Global.LOGGER.info("-----------------------------------------------------------------------------");
				Global.LOGGER.info("Data migration strategy: "+Global.dataMigrationStrategy);
				
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
}