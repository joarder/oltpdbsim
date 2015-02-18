package main.java.workload;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Map.Entry;

import main.java.cluster.Cluster;
import main.java.cluster.Server;
import main.java.db.Tuple;
import main.java.db.Database;
import main.java.db.Table;
import main.java.entry.Global;
import main.java.utils.graph.SimpleVertex;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.math3.distribution.ZipfDistribution;

public class Workload implements java.io.Serializable {	

	private static final long serialVersionUID = 1L;
	
	public boolean warmingup;
	
	public int tbl = 0;		
	public Map<Integer, Integer> tbl_types = null;
	public Map<Integer, ArrayList<Integer>> schema = null;
	
	public Map<Integer, Double> tr_proportions = null;
	Map<Integer, ArrayList<Integer>> tr_tuple_distributions = null;	
	Map<Integer, ArrayList<Integer>> tr_changes = null;
	String file_name = null;
	
	public int tr_types = 0;
	
	// new
	public int[] trTypes = null;
	public double[] trProbabilities = null;
	
	// Cache
	protected HashMap<Integer, ArrayList<Integer>> _cache;
	protected ArrayList<Integer> _cache_keys;
	protected int _cache_id;		
	
	Workload(String file) {
		file_name = file;
		BufferedReader config_file = null; 
	    AbstractFileConfiguration parameters = null;
	    
	    try {
	    	config_file = new BufferedReader(
	    			new InputStreamReader(getClass().getResourceAsStream(file_name)));
	    	
			Global.LOGGER.info("Configuration file "+file_name+" is found under src/main/resources and read.");
	    	
		    //Load configuration parameters
	    	parameters = new PropertiesConfiguration();
			parameters.load(config_file);
						
			//Read the number of tables and types of transactions
			tbl = parameters.getInt("tables");
			tr_types = parameters.getInt("transaction.types");
			
			Global.LOGGER.info("Number of database tables: "+tbl);
			Global.LOGGER.info("Types of workload transactions: "+tr_types);
			
		} catch (ConfigurationException e) {
			e.printStackTrace();
		} finally {
			if(config_file != null) {
				try {
					config_file.close();			
				} catch (IOException e) {
					e.printStackTrace();
				}			
			}
		}
	    
	    //Initialize other variables
	    this.warmingup = false;
	    this.tbl_types = new HashMap<Integer, Integer>();
	    this.schema = new HashMap<Integer, ArrayList<Integer>>();
	    this.tr_proportions = new HashMap<Integer, Double>();
	    this.tr_tuple_distributions = new HashMap<Integer, ArrayList<Integer>>();	    
	    this.tr_changes = new HashMap<Integer, ArrayList<Integer>>();	    
	    this._cache = new HashMap<Integer, ArrayList<Integer>>();
		this._cache_keys = new ArrayList<Integer>();
		this._cache_id = 0;
	}
	
	// Generate data set and create a new Transaction
	public Set<Integer> getTrTupleSet(Database db, int type) {		
		return this.getTrTupleSet(db, type);		
	}
	
	//Generates Zipf Ranks for the Primary Tables
	public void generateDataPopularity(Database db) {
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Generating data popularity profiles for primary tables ...");
		
		double exponent = 2.0; // 2~3
		
		for(Entry<Integer, Table> tbl_entry : db.getDb_tables().entrySet()) {
			
			Table tbl = tbl_entry.getValue();
			
	    	if(tbl.getTbl_type() == 0)
	    		this.getZipfProbability(tbl, exponent);	    	
		}
		
		Global.LOGGER.info("Data popularity profile generation has been completed.");
	}
	
	// Calculate Zipf probability P(X = x) for all the Data tuples in a Table following Zipf Distribution	
	private void getZipfProbability(Table tbl, double exponent) {		
		ZipfDistribution zipf = new ZipfDistribution(tbl.getTbl_init_tuples(), exponent);
		zipf.reseedRandomGenerator(Global.repeated_runs);
		
		int d = 1;	
		for(Entry<Integer, Tuple> data : tbl.getTbl_tuples().entrySet()) {
			tbl.getTbl_dataRank()[(d % tbl.getTbl_tuples().size())+1] 
					= data.getValue().getTuple_pk();
			
			d++;
		}			
	}
	
	public static Set<Integer> getTrDataSet(Database db, Cluster cluster, WorkloadBatch wb, 
			Set<Integer> trTupleSet) {
		
		Set<Integer> trDataSet = new TreeSet<Integer>();
		SortedSet<Integer> deletedTuples = new TreeSet<Integer>();
		
		int s_id = 1;
		for(int tpl_id : trTupleSet) {							
			
			if(!deletedTuples.contains(tpl_id)) {
			
				String[] parts = cluster.breakDataId(tpl_id);					
				int tpl_pk = Integer.parseInt(parts[0]);
				int tbl_id = Integer.parseInt(parts[1]);
				
				Tuple tpl = db.getTupleById(tbl_id, tpl_id);
				int _id = -1;
				
				if(tpl.getTuple_action().equals("insert")) {
					
					tpl.setTuple_action("initial");
					
					// Insert into Cluster, already in the Database
					
					switch(Global.setup) {
						case "range":					
							Server s = cluster.getServer(s_id);
							int p_id = cluster.getRangePartition(s, tbl_id);
							
							_id = cluster.insertDataRangePartitioning(tpl_id, s_id, p_id);
							
							++s_id;
							if(s_id > Global.servers)
								s_id = 1;
							
							break;
						
						case "consistenthash":
							_id = cluster.insertDataConsistentHash(tpl_id);
							break;
							
						default:
							Global.LOGGER.error("Wrong cluster setup method is specified !!! Choose either 'range' or 'consistenthash'");
							break;
					}					
													
					trDataSet.add(_id);					
					
				} else if (tpl.getTuple_action().equals("delete")) {
					
					if(!Cluster._setup) {
					
						 // Remove from Cluster
						switch(Global.setup) {
							case "range":					
								cluster.deleteDataRangePartitioning(tpl_id);								
								break;
							
							case "consistenthash":
								cluster.deleteDataConsistentHashing(tpl_id);
								break;
								
							default:
								Global.LOGGER.error("Wrong cluster setup method is specified !!! Choose either 'range' or 'consistenthash'");
								break;
						}
						
						 // Remove from Database
						db.deleteTupleByPk(tbl_id, tpl_pk);
											
						_id = cluster.getDataIdFromTupleId(tpl_id);
						
						// Remove from Workload Batch												
						// Removing vertex in Workload Batch graph and hypergraph						
						wb.deleteTrDataFromWorkload(_id);
						
						SimpleVertex v = wb.hgr.getVertex(_id);
						if(v != null) {
							wb.hgr.removeVertex(v);
							
							if(Global.compressionEnabled)
				        		wb.hgr.removeCVertex(v);
						}
						
						deletedTuples.add(tpl_id);			
					}
					
				} else {
					
					_id = cluster.getDataIdFromTupleId(tpl_id);
					trDataSet.add(_id);
				}				
			} // end--if()
		} // Tuple
	
		return trDataSet;
	}
}