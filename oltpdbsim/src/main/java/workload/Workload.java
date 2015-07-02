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

import main.java.cluster.Cluster;
import main.java.cluster.Server;
import main.java.db.Database;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.utils.graph.SimpleVertex;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class Workload {
	
	public boolean warmingup;
	
	public int tbl = 0;		
	public Map<Integer, Integer> tbl_types = null;
	public Map<Integer, ArrayList<Integer>> schema = null;
	
	public Map<Integer, Double> tr_proportions = null;
	protected Map<Integer, ArrayList<Integer>> tr_tuple_distributions = null;	
	protected Map<Integer, ArrayList<Integer>> tr_changes = null;
	private String file_name = null;
	
	public int tr_types = 0;
	
	// new
	public int[] trTypes = null;
	public double[] trProbabilities = null;
	
	// Cache
	protected HashMap<Integer, ArrayList<Integer>> _cache;
	protected ArrayList<Integer> _cache_keys;
	protected int _cache_id;		
	
	protected Workload(String file) {
		setFile_name(file);
		BufferedReader config_file = null; 
	    AbstractFileConfiguration parameters = null;
	    
	    try {
	    	config_file = new BufferedReader(
	    			new InputStreamReader(getClass().getResourceAsStream(getFile_name())));
	    	
			Global.LOGGER.info("Configuration file "+getFile_name()+" is found under src/main/resources and read.");
	    	
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
	
	public static Set<Integer> getTrDataSet(Database db, Cluster cluster, WorkloadBatch wb, 
			Set<Integer> trTupleSet) {
		
		Set<Integer> trDataSet = new TreeSet<Integer>();
		SortedSet<Integer> deletedTuples = new TreeSet<Integer>();
		
		int s_id = 1;
		for(int tpl_id : trTupleSet) {							
			
			if(!deletedTuples.contains(tpl_id)) {
			
				String[] parts = Cluster.breakDataIdWithoutReplicaId(tpl_id);					
				int tpl_pk = Integer.parseInt(parts[0]);
				int tbl_id = Integer.parseInt(parts[1]);
				
				Tuple tpl = db.getTupleById(tbl_id, tpl_id);
				int _id = -1;
				
				if(tpl.getTuple_action().equals(WorkloadConstants.TPL_INSERT)) {
					
					tpl.setTuple_action(WorkloadConstants.TPL_INITIAL);
					
					// Insert into Cluster, already in the Database					
					switch(Global.setup) {
						case "range":					
							Server s = cluster.getServer(s_id);
							int p_id = cluster.getRangePartition(s, tbl_id);
							
							_id = cluster.insertData_RangePartitioning(tpl_id, s_id, p_id);							
							
							++s_id;
							if(s_id > Global.servers)
								s_id = 1;
							
							break;
						
						case "consistenthash":
							_id = cluster.insertData_ConsistentHash(tpl_id);
							break;
							
						default:
							Global.LOGGER.error("Wrong cluster setup method is specified !!! Choose either 'range' or 'consistenthash'");
							break;
					}					
													
					trDataSet.add(_id);					
					
				} else if (tpl.getTuple_action().equals("delete")) {
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
										
					_id = Cluster.getDataIdFromTupleId(tpl_id);
					
					// Remove from Workload Batch												
					// Removing vertex in Workload Batch graph and hypergraph						
					wb.deleteTrDataFromWorkload(_id);
					
					SimpleVertex v = wb.hgr.getVertex(_id);
					if(v != null) {
						wb.hgr.removeVertex(v);
					}
					
					deletedTuples.add(tpl_id);
									
				} else {
					
					_id = Cluster.getDataIdFromTupleId(tpl_id);
					trDataSet.add(_id);
				}				
			} // end--if()
		} // Tuple
	
		return trDataSet;
	}

	public String getFile_name() {
		return file_name;
	}

	public void setFile_name(String file_name) {
		this.file_name = file_name;
	}

	public void readConfig() {}
}