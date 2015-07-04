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

package main.java.workload.tpcc;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import main.java.db.Database;
import main.java.db.Table;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.workload.Workload;
import main.java.workload.WorkloadConstants;

import org.apache.commons.configuration.AbstractFileConfiguration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;

public class TpccWorkload extends Workload {
	
	public TpccWorkload(String file) {	
		super(file);				
	}

	// Read TPCC configuration file
	public void readConfig() {
		BufferedReader config_file = null; 
	    AbstractFileConfiguration parameters = null;
	    int i, j = 0;
	    
	    try {
	    	config_file = new BufferedReader(new InputStreamReader(getClass().getResourceAsStream(this.getFile_name())));
	    	
		//Load configuration parameters
	    	parameters = new PropertiesConfiguration();
			parameters.load(config_file);
			
		//Read TPCC scale
			//WorkloadConstants.SCALE_FACTOR = parameters.getDouble("tpcc.scale");
			WorkloadConstants.SCALE_FACTOR = Global.scaleFactor;
			Global.LOGGER.info("TPCC scale: "+WorkloadConstants.SCALE_FACTOR);
		 
		//Read TPCC warehouses
			TpccConstants.NUM_WAREHOUSES = parameters.getInt("tpcc.warehouses");	    	
			Global.LOGGER.info("TPCC warehouses: "+TpccConstants.NUM_WAREHOUSES);
			
		//Read TPCC table types
			i = 1;
			for(Object param : parameters.getList("tpcc.tbl.type")) {
				tbl_types.put(i, Integer.parseInt((String) param));				
				++i;
			}			
			Global.LOGGER.info("TPCC tables types: "+tbl_types);
			
		//Read TPCC schema
			i = j = 0;
			ArrayList<Integer> temp = null;
			for(Object param : parameters.getList("tpcc.schema")) {				
				if(j >= 9 || j == 0){
					++i;
					j = 0;
					temp = new ArrayList<Integer>();
					schema.put(i, temp);					
				}
				
				schema.get(i).add(Integer.parseInt((String)param));
				++j;
			}
			Global.LOGGER.info("TPCC table-level schema: "+schema);
			
		//Read TPCC transaction proportion
			i = 0;
			//new 
			this.trTypes = new int[tr_types];
			this.trProbabilities = new double[tr_types];
			
			for(Object param : parameters.getList("tpcc.trs.proportions")) {
				++i;
				tr_proportions.put(i, Double.parseDouble((String) param));
				
				// new
				this.trTypes[i-1] = i;
				this.trProbabilities[i-1] = Double.parseDouble((String) param);
			}
			Global.LOGGER.info("TPCC transaction proportions: "+tr_proportions);
			
		//Read TPCC transactions
			i = j = 0;
			temp = null;
			for(Object param : parameters.getList("tpcc.trs.tbl_data")) {				
				if(j >= 9 || j == 0){					
					++i;
					j = 0;
					temp = new ArrayList<Integer>();
					tr_tuple_distributions.put(i, temp);					
				}
				
				tr_tuple_distributions.get(i).add(Integer.parseInt((String)param));
				++j;
			}
			Global.LOGGER.info("TPCC transaction table-level data distributions: "+tr_tuple_distributions);
			
		//Read TPCC transactional changes
			i = j = 0;
			temp = null;
			for(Object param : parameters.getList("tpcc.trs.tbl_changes")) {				
				if(j >= 9 || j == 0){					
					++i;
					j = 0;
					temp = new ArrayList<Integer>();
					tr_changes.put(i, temp);					
				}
				
				tr_changes.get(i).add(Integer.parseInt((String)param));
				++j;
			}
			Global.LOGGER.info("TPCC table-level transactional changes: "+tr_changes);			
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
		
		Set<Integer> trTupleSet = new HashSet<Integer>();
		
		ArrayList<Integer> trTupleList = new ArrayList<Integer>();
		ArrayList<Integer> tupleList = null;
		ArrayList<Integer> keyList;		
		ArrayList<Integer> _cache_items = null;
		
		int tuple_nums, action = 0; 
		int tpl_pk = Global.global_tupleSeq;
		int d_id = 0;
		int _w = 1, _i = 1, _d = 1, _s = 1, _c = 1, _o = 1, _no = 1, _ol = 1, cache_key = 1;
		
		//System.out.println("--> Generating a transaction of type "+tr_type+" ...");
		for(Entry<Integer, Table> tbl_entry : db.getDb_tables().entrySet()) {
			
			Table tbl = tbl_entry.getValue();
			
			tuple_nums = this.tr_tuple_distributions.get(tr_type).get(tbl.getTbl_id() - 1);
			action = this.tr_changes.get(tr_type).get(tbl.getTbl_id() - 1);
			
			//System.out.println("--> Table("+tbl.getTbl_name()+") | data = "+tbl.getTbl_tuples().size()+" | action = "+action+" | tuple_nums = "+tuple_nums);
			
			switch(action) {
			case 0:						
					if(tbl.getTbl_tuples().size() == 1 && tuple_nums != 0) { // e.g. for <Warehouse> table; W=1						
						for(Entry<Integer, Tuple> data : tbl.getTbl_tuples().entrySet()) {
							trTupleSet.add(data.getValue().getTuple_pk());
							_w = data.getValue().getTuple_pk();
						}
					} else {
						for(int d = 1; d <= tuple_nums; d++) {
							//================new
							switch(tbl.getTbl_name()) {
								case TpccConstants.TBL_WAREHOUSE:
									//_w = Global.rand.nextInt(tbl.getTbl_tuples().size())+1;
									_w = tbl.zipfDistribution.sample();
									tupleList = new ArrayList<Integer>();
									tupleList.add(_w);
									
									//System.out.println("\t\t--> W("+_w+")");
									break;
									
								case TpccConstants.TBL_ITEM:
									_i = tbl.zipfDistribution.sample();
									
									// Re-seeding to change Items' popularity
									/*if(Sim.time() >= timer) {			
										timer += timer;
										tbl.zipfDistribution.reseedRandomGenerator(++seed);
									}*/
									
									tupleList = new ArrayList<Integer>();
									tupleList.add(_i);
									
									//System.out.println("\t\t--> I("+_i+")");
									break;
									
								case TpccConstants.TBL_DISTRICT:
									if(tr_type <= 1 || this._cache.isEmpty()) {
										keyList = new ArrayList<Integer>();
										keyList.add(_w);
										tupleList = tbl.getTableData(keyList);										
										_d = tupleList.get(1);
									} else {
										int index = Global.rand.nextInt(_cache.size());
										cache_key = this._cache_keys.get(index);
										_cache_items = this._cache.get(cache_key);
										
										_d = _cache_items.get(0);
										_c = _cache_items.get(1);
										_o = _cache_items.get(2);
										_no = _cache_items.get(3);
										_ol = _cache_items.get(4);
										_s = _cache_items.get(5);
										
										//System.out.println("\t\t>> Retrieving cached data <D("+_d+")|C("+_c+")|O("+_o+")|NO("+_no+")|OL("+_ol+")|S("+_s+")>");
										
										tupleList = new ArrayList<Integer>();
										tupleList.add(_d);
									}
									
									//System.out.println("\t\t--> D("+_d+") for W("+_w+")");
									break;
									
								case TpccConstants.TBL_STOCK:
									if(tr_type <= 1 || this._cache.isEmpty()) {
										keyList = new ArrayList<Integer>();
										keyList.add(_w);
										keyList.add(_i);
										tupleList = tbl.getTableData(keyList);
										_s = tupleList.get(1);
									} else {
										tupleList = new ArrayList<Integer>();
										tupleList.add(_s);
									}
									
									//System.out.println("\t\t--> S("+_s+") for W("+_w+") and I("+_i+")");
									break;
									
								case TpccConstants.TBL_CUSTOMER: // District Table
									if(tr_type <= 1 || this._cache.isEmpty()) {
										keyList = new ArrayList<Integer>();
										keyList.add(_d);
										tupleList = tbl.getTableData(keyList);
										_c = tupleList.get(1);										
									} else {										
										_cache_items = new ArrayList<Integer>();
										int index = Global.rand.nextInt(_cache.size());
										cache_key = this._cache_keys.get(index);									
										_cache_items = this._cache.get(cache_key);
										
										_d = _cache_items.get(0);
										_c = _cache_items.get(1);
										_o = _cache_items.get(2);
										_no = _cache_items.get(3);
										_ol = _cache_items.get(4);
										_s = _cache_items.get(5);
										
										//System.out.println("\t\t>> Retrieving cached data <D("+_d+")|C("+_c+")|O("+_o+")|NO("+_no+")|OL("+_ol+")|S("+_s+")>");
										
										tupleList = new ArrayList<Integer>();
										tupleList.add(_c);
									}
									
									//System.out.println("\t\t--> C("+_c+") for D("+_d+") -- "+tupleList.get(0));										
									break;
									
								case TpccConstants.TBL_HISTORY: // Customer Table
									// Nothing to do
									break;
									
								case TpccConstants.TBL_ORDERS:
									if(tr_type <= 1 || this._cache.isEmpty()) {
										keyList = new ArrayList<Integer>();
										keyList.add(_c);
										tupleList = tbl.getTableData(keyList);
										_o = tupleList.get(1);
									} else {										
										d_id = tbl.getTupleByPk(_o).getTuple_pk();										
										tupleList = new ArrayList<Integer>();										
										tupleList.add(d_id);
										tupleList.add(_o);										
									}
									
									//System.out.println("\t\t--> O("+_o+") for C("+_c+") -- "+tupleList.get(0));
									break;
									
								case TpccConstants.TBL_NEW_ORDER: // Customer Table (the last 1/3 values from the Order table)					
									// Nothing to do
									break;
									
								case TpccConstants.TBL_ORDER_LINE: // Stock Table (10 most popular values from the Stock table)
									keyList = new ArrayList<Integer>();
									keyList.add(_s);
									keyList.add(_o);
									tupleList = tbl.getTableData(keyList);
									_ol = tupleList.get(0);									
									
									//System.out.println("\t\t--> OL("+_ol+") for S("+_s+") and O("+_o+") -- "+tupleList.get(0));
									break;
							}
														
							if(trTupleList.contains(tupleList.get(0)) && d > 1) {
								--d;
								
							} else {
								trTupleList.add(tupleList.get(0));
								//System.out.println("@ "+tupleList.get(0));
								trTupleSet.add(tbl.getTupleByPk(tupleList.get(0)).getTuple_id());
							}
						}
					}
					break;
			
			case 1:	//===Insert Operation				
					// Create a new Tuple
					tpl_pk = tbl.getTbl_last_entry();
					++tpl_pk;
					
					tbl.setTbl_last_entry(tpl_pk);
					++Global.global_tupleSeq;
				
				// Insert
					Tuple tpl = db.insertTuple(tbl.getTbl_id(), tpl_pk);
					trTupleSet.add(tpl.getTuple_id()); // Add Tuple id
					
					if(!this.warmingup) 
						tpl.setTuple_action(WorkloadConstants.TPL_INSERT);
					
					switch(tbl.getTbl_name()) {
						case(TpccConstants.TBL_HISTORY):
							//tpl.getTuple_fk().put(3, _d); // 3: District Table						
							// 3: District Table
							if(tpl.getTuple_fk().containsKey(3))
								tpl.getTuple_fk().get(3).add(_d);
							else {
								Set<Integer> fkSet = new HashSet<Integer>();
								fkSet.add(_d);
								tpl.getTuple_fk().put(3, fkSet);
							}
						
							//tpl.getTuple_fk().put(5, _c); // 5: Customer Table
							// 5: Customer Table
							if(tpl.getTuple_fk().containsKey(5))
								tpl.getTuple_fk().get(5).add(_c);
							else {
								Set<Integer> fkSet = new HashSet<Integer>();
								fkSet.add(_c);
								tpl.getTuple_fk().put(5, fkSet);
							}
							
							//System.out.println("\t\t>> Inserting _h(x) for _c("+_c+") and _d("+_d+")");
							
							tbl.getIdx_multikey_dependent().put(_d, _c, tbl.getTbl_tuples().size());
							break;
						
						case(TpccConstants.TBL_ORDERS):
							_o = tbl.getTbl_last_entry();
							//System.out.println("\t\t>> Inserting _o("+_o+") for _c("+_c+") and _d("+_d+")");
						
							//tpl.getTuple_fk().put(5, _c); // 5: Customer Table
						
							// 5: Customer Table
							if(tpl.getTuple_fk().containsKey(5))
								tpl.getTuple_fk().get(5).add(_c);
							else {
								Set<Integer> fkSet = new HashSet<Integer>();
								fkSet.add(_c);
								tpl.getTuple_fk().put(5, fkSet);
							}
						
							// Insert into Index
							tbl.insertSecondaryIdx(_c, _o);						
							
						//=== Also put an entry in the New-Order and Order-Line Table														
						//=== New-Order
							Table _no_tbl = db.getTable(8);
							
							int _no_tpl_pk = _no_tbl.getTbl_last_entry();
							++_no_tpl_pk;
							_no_tbl.setTbl_last_entry(_no_tpl_pk);
							
							++Global.global_tupleSeq;
						
						// Insert								
							Tuple _no_tpl = db.insertTuple(_no_tbl.getTbl_id(), _no_tpl_pk);
							trTupleSet.add(_no_tpl.getTuple_id()); // Add Tuple id
							
							if(!this.warmingup) 
								_no_tpl.setTuple_action(WorkloadConstants.TPL_INSERT);	
							
							_no = _no_tbl.getTbl_last_entry();
							_no_tpl.setTuple_pk(_no);
							//_no_tpl.getTuple_fk().put(7, _o); // 7: Orders Table						
							
							// 7: Orders Table
							if(tpl.getTuple_fk().containsKey(7))
								tpl.getTuple_fk().get(7).add(_o);
							else {
								Set<Integer> fkSet = new HashSet<Integer>();
								fkSet.add(_o);
								tpl.getTuple_fk().put(7, fkSet);
							}
								
							//System.out.println("\t\t>> Inserting _no("+_no+") for _o("+_o+")");
							
							// Insert into Index
							_no_tbl.insertSecondaryIdx(_o, _no_tpl_pk);
							
						//=== Order-Line
							Table _ol_tbl = db.getTable(9);
							
							int _ol_tpl_pk = _ol_tbl.getTbl_last_entry();
							++_ol_tpl_pk;
							_ol_tbl.setTbl_last_entry(_ol_tpl_pk); // Add Tuple id
							
							++Global.global_tupleSeq;
						
						// Insert								
							Tuple _ol_tpl = db.insertTuple(_ol_tbl.getTbl_id(), _ol_tpl_pk);
							trTupleSet.add(_ol_tpl.getTuple_id());
							if(!this.warmingup) _ol_tpl.setTuple_action(WorkloadConstants.TPL_INSERT);
														
							_ol = _ol_tbl.getTbl_last_entry();
							_ol_tpl.setTuple_pk(_ol);
							//_ol_tpl.getTuple_fk().put(4, _s); // 4: Stock Table
							
							// 4: Stock Table
							if(tpl.getTuple_fk().containsKey(4))
								tpl.getTuple_fk().get(4).add(_s);
							else {
								Set<Integer> fkSet = new HashSet<Integer>();
								fkSet.add(_s);
								tpl.getTuple_fk().put(4, fkSet);
							}
							
							//_ol_tpl.getTuple_fk().put(7, _o); // 7: Orders Table
							// 7: Orders Table
							if(tpl.getTuple_fk().containsKey(7))
								tpl.getTuple_fk().get(7).add(_o);
							else {
								Set<Integer> fkSet = new HashSet<Integer>();
								fkSet.add(_o);
								tpl.getTuple_fk().put(7, fkSet);
							}
							
							//System.out.println("\t\t>> Inserting _ol("+_ol+"|"+ol_tuple_pk+"|"+_ol_tpl.getData_pk()+") for _s("+_s+") and _o("+_o+")");
							
							_ol_tbl.getIdx_multikey_dependent().put(_s, _o, _ol_tpl_pk);
							
							// Caching
							_cache_items = new ArrayList<Integer>();
							_cache_items.add(_d);
							_cache_items.add(_c);
							_cache_items.add(_o);
							_cache_items.add(_no);
							_cache_items.add(_ol);
							_cache_items.add(_s);
							
							boolean _cached = false;
							for(Entry<Integer, ArrayList<Integer>> entry : this._cache.entrySet()) {
								if(_cache_items.equals(entry.getValue())) {
									_cached = true;
									break;
								}
							}
							
							if(!_cached) {
								this._cache.put(this._cache_id, _cache_items);
								this._cache_keys.add(this._cache_id);
								this._cache_id++;
							}
							
							//System.out.println("\t\t>> Caching D-"+_d+"|C-"+_c+"|O-"+_o+"|NO-"+_no+"|OL-"+_ol+"|S-"+_s);							
							break;													
					}			
											
					break;
					
			case -1://===Delete Operation					
					//System.out.println("\t\t>> *** _d("+_d+")|_c("+_c+")|_o("+_o+")|_no("+_no+")|_ol("+_ol+")|_s("+_s+")");
					
					// Mark the Tuple as "delete"
					Tuple tuple = db.getTupleByPk(tbl.getTbl_id(), _no);
					trTupleSet.add(tuple.getTuple_id()); // Add Tuple id
					
					if(!this.warmingup) 
						tuple.setTuple_action(WorkloadConstants.TPL_DELETE);
					
					// Remove from Table index
					tbl.getIdx_multivalue_secondary().remove(_o);

					// Remove from Database
					//db.deleteTupleByPk(tbl.getTbl_id(), _no);
					
					// Remove from Cache
					this._cache_keys.remove(new Integer(cache_key));
					this._cache.remove(cache_key);
					//System.out.println("\t\t@ Removed D("+_d+") from cache | index ("+cache_key+")"+"|cache size="+_cache.size());
					
					// Remove all occurrences the tuple from the workload transactions by its pk
					//deleteTupleFromWorkload(tbl.getTbl_id(), tpl_pk); // Actually not required. Will remove in future
					
					break;
			} //end--switch()
		} //end--for()
		
		return trTupleSet;
	}	
}