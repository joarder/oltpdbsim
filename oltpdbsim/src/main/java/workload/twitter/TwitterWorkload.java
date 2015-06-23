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
 
package main.java.workload.twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Set;

import main.java.db.Database;
import main.java.entry.Global;
import main.java.workload.Workload;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class TwitterWorkload extends Workload {
	
	public double scale = 0.0;
	
	public TwitterWorkload(String file) {	
		super(file);				
	}

	// Read Twitter configuration file
	public void readConfig() {
		BufferedReader config_file = null; 
	    AbstractFileConfiguration parameters = null;
	    int i, j = 0;
	    
	    try {
	    	config_file = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(this.getFile_name())));
	    	
		//Load configuration parameters
	    	parameters = new PropertiesConfiguration();
			parameters.load(config_file);
			
		//Read Twitter scale
			scale = parameters.getDouble("twitter.scale");		    	
			Global.LOGGER.info("Twitter scale: "+scale);
			
		//Read Twitter table types
			i = 1;
			for(Object param : parameters.getList("twitter.tbl.type")) {
				tbl_types.put(i, Integer.parseInt((String) param));				
				++i;
			}
			
			Global.LOGGER.info("Twitter tables types: "+tbl_types);
			
		//Read Twitter schema
			i = j = 0;
			ArrayList<Integer> temp = null;
			for(Object param : parameters.getList("twitter.schema")) {				
				if(j >= 5 || j == 0){
					++i;
					j = 0;
					temp = new ArrayList<Integer>();
					schema.put(i, temp);					
				}
				
				schema.get(i).add(Integer.parseInt((String)param));
				++j;
			}
			
			Global.LOGGER.info("Twitter table-level schema: "+schema);
		
		//Read Twitter transaction proportion
			i = 0;
			//new 
			this.trTypes = new int[tr_types];
			this.trProbabilities = new double[tr_types];
			
			for(Object param : parameters.getList("twitter.trs.proportions")) {
				++i;
				tr_proportions.put(i, Double.parseDouble((String) param));
				
				// new
				this.trTypes[i-1] = i;
				this.trProbabilities[i-1] = Double.parseDouble((String) param);
			}
			
			Global.LOGGER.info("Twitter transaction proportions: "+tr_proportions);
			
		//Read Twitter transactions
			i = j = 0;
			temp = null;
			for(Object param : parameters.getList("twitter.trs.tbl_data")) {				
				if(j >= 5 || j == 0){					
					++i;
					j = 0;
					temp = new ArrayList<Integer>();
					tr_tuple_distributions.put(i, temp);					
				}
				
				tr_tuple_distributions.get(i).add(Integer.parseInt((String)param));
				++j;
			}
			Global.LOGGER.info("Twitter transaction table-level data distributions: "+tr_tuple_distributions);
			
		//Read Twitter transactional changes
			i = j = 0;
			temp = null;
			for(Object param : parameters.getList("twitter.trs.tbl_changes")) {				
				if(j >= 5 || j == 0){					
					++i;
					j = 0;
					temp = new ArrayList<Integer>();
					tr_changes.put(i, temp);					
				}
				
				tr_changes.get(i).add(Integer.parseInt((String)param));
				++j;
			}
			Global.LOGGER.info("Twitter table-level transactional changes: "+tr_changes);			
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
	}	
		
	@Override
	// Creates a Tuple set for a specific transaction of type i
	public Set<Integer> getTrTupleSet(Database db, int tr_type) {
		
		
		
		return null;		
	}
}