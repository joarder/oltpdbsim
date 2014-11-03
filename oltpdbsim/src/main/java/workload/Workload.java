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
import main.java.db.Tuple;
import main.java.db.Database;
import main.java.db.Table;
import main.java.entry.Global;

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
		
		for(int tpl_id : trTupleSet) {							
			
			if(!deletedTuples.contains(tpl_id)) {
			
				String[] parts = cluster.breakDataId(tpl_id);					
				int tpl_pk = Integer.parseInt(parts[0]);
				int tbl_id = Integer.parseInt(parts[1]);
				
				Tuple tpl = db.getTupleById(tbl_id, tpl_id);
				int data_id = 0;						
				
				if(tpl.getTuple_action().equals("insert")) {
					
					tpl.setTuple_action("initial");
					
					// Insert into Cluster, already in the Database
					data_id = cluster.insertData(tpl_id);					
					trDataSet.add(data_id);
					
				} else if (tpl.getTuple_action().equals("delete")) {
					
					if(!Cluster._setup) {
						
						cluster.deleteData(tpl_id); // Remove from Cluster
						db.deleteTupleByPk(tbl_id, tpl_pk); // Remove from Database
												
						// Remove from Workload Batch												
						// Removing vertex in Workload Batch graph and hypergraph
						data_id = cluster.getDataIdFromTupleId(tpl_id);
						wb.deleteTrDataFromWorkload(data_id);
						
						switch(Global.workloadRepresentation) {
							case "gr":
								if(wb.gr.getVertex(data_id) != null)
									wb.gr.removeVertex(wb.gr.getVertex(data_id));
								
								break;
								
							case "hgr":
								if(wb.hgr.getVertex(data_id) != null)
									wb.hgr.removeVertex(wb.hgr.getVertex(data_id));
								
								break;
						}
						
						deletedTuples.add(tpl_id);						
					}
					
				} else {
					
					data_id = cluster.getDataIdFromTupleId(tpl_id);
					trDataSet.add(data_id);
				}				
			} // end--if()
		} // Tuple
		
		return trDataSet;
	}
}