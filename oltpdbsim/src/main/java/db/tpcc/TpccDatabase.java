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

package main.java.db.tpcc;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;

import main.java.db.Database;
import main.java.db.Table;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.utils.Utility;
import main.java.workload.Workload;
import main.java.workload.tpcc.TpccConstants;
import main.java.workload.tpcc.TpccWorkload;

import org.apache.commons.math3.distribution.ZipfDistribution;

public class TpccDatabase extends Database {

	public TpccDatabase(String name) {
		super(name);
	}
		
	// Estimate initial database Table sizes
	private void estimateTableSize(TpccWorkload wrl) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Estimating initial data tuple counts for "+this.getDb_name()+" database ...");
		
		String[] table_names = {
				TpccConstants.TBL_WAREHOUSE,
				TpccConstants.TBL_ITEM,
				TpccConstants.TBL_DISTRICT,
				TpccConstants.TBL_STOCK,
				TpccConstants.TBL_CUSTOMER,
				TpccConstants.TBL_HISTORY,
				TpccConstants.TBL_ORDERS,
				TpccConstants.TBL_NEW_ORDER,
				TpccConstants.TBL_ORDER_LINE
		};
		
		int dependent = 0;
		
		for(int tbl_id = 1; tbl_id <= wrl.tbl; tbl_id++) {
			Table tbl = new Table(tbl_id, wrl.tbl_types.get(tbl_id), table_names[tbl_id-1]);
			this.getDb_tbl_name_id_map().put(tbl.getTbl_name(), tbl.getTbl_id());
			
			// Set TPCC table ranks for Warehouse and Item tables
			if(tbl.getTbl_name().equals(TpccConstants.TBL_WAREHOUSE)) {
				tbl.setTbl_data_rank(new int[TpccConstants.NUM_WAREHOUSES + 1]);
				
				tbl.zipfDistribution = new ZipfDistribution(TpccConstants.NUM_WAREHOUSES, TpccConstants.ZIPF_EXP);
				tbl.zipfDistribution.reseedRandomGenerator(Global.repeated_runs);
				
			} else if(tbl.getTbl_name().equals(TpccConstants.TBL_ITEM)) {
				tbl.setTbl_data_rank(new int[((int) (TpccConstants.NUM_ITEMS * TpccConstants.SCALE_FACTOR)) + 1]);
				
				tbl.zipfDistribution = new ZipfDistribution(((int) (TpccConstants.NUM_ITEMS * TpccConstants.SCALE_FACTOR)), TpccConstants.ZIPF_EXP);
				tbl.zipfDistribution.reseedRandomGenerator(Global.repeated_runs);				
			}
			
			this.getDb_tables().put(tbl_id, tbl);			
			
			// Determine the number of Data rows to be populated for each individual table
			switch(tbl.getTbl_name()) {
				case TpccConstants.TBL_WAREHOUSE:
					tbl.setTbl_init_tuples(TpccConstants.NUM_WAREHOUSES);					
					break;
					
				case TpccConstants.TBL_ITEM:
					tbl.setTbl_init_tuples((int) (TpccConstants.NUM_ITEMS * TpccConstants.SCALE_FACTOR));					
					break;
					
				case TpccConstants.TBL_DISTRICT:
					tbl.setTbl_init_tuples((int) (TpccConstants.DISTRICTS_PER_WAREHOUSE * TpccConstants.NUM_WAREHOUSES));
					break;
					
				case TpccConstants.TBL_STOCK:
					tbl.setTbl_init_tuples((int) (TpccConstants.STOCKS_PER_WAREHOUSE * TpccConstants.NUM_WAREHOUSES * TpccConstants.SCALE_FACTOR));
					break;
					
				case TpccConstants.TBL_CUSTOMER: // Relating District Table
					tbl.setTbl_init_tuples((int) (TpccConstants.CUSTOMERS_PER_DISTRICT * this.getTable(this.getDb_tbl_name_id_map().get(TpccConstants.TBL_DISTRICT)).getTbl_init_tuples() * TpccConstants.SCALE_FACTOR));
					break;
					
				case TpccConstants.TBL_HISTORY: // Relating Customer Table (1+) // CUSTOMER
					tbl.setTbl_init_tuples(this.getTable(this.getDb_tbl_name_id_map().get(TpccConstants.TBL_CUSTOMER)).getTbl_init_tuples()); 
					break;
					
				case TpccConstants.TBL_ORDERS: // Relating Customer Table (1+)
					tbl.setTbl_init_tuples((int) (this.getTable(this.getDb_tbl_name_id_map().get(TpccConstants.TBL_CUSTOMER)).getTbl_init_tuples()));
					break;
					
				case TpccConstants.TBL_NEW_ORDER: // Relating Order Table (the last 1/3 values from the Order Table) //ORDERS					
					tbl.setTbl_init_tuples(
							(int) (Math.pow(10.0d, (Utility.getBase(this.getTable(this.getDb_tbl_name_id_map().get(TpccConstants.TBL_ORDERS)).getTbl_init_tuples()) -1)) 
									- Math.pow(10.0d, (Utility.getBase(this.getTable(this.getDb_tbl_name_id_map().get(TpccConstants.TBL_ORDERS)).getTbl_init_tuples()) -2))));					
					break;
					
				case TpccConstants.TBL_ORDER_LINE: // Relating Order and Stock Table (10 most popular values from the Stock table) //ORDERS
					//tbl.setTbl_init_tuples(this.getTable(this.getDb_tbl_name_id_map().get(TpccConstants.TBL_HISTORY)).getTbl_init_tuples() * TpccConstants.NUM_MOST_POPULAR_ITEM_FROM_STOCK);
					tbl.setTbl_init_tuples(this.getTable(6).getTbl_init_tuples() * 10);
					break;
			}
			
			Global.LOGGER.info(tbl.getTbl_name()+": "+tbl.getTbl_init_tuples());
			
			// Setting dependency information for the database tables
			if(tbl.getTbl_type() != 0){
				for(int i = 1; i <= wrl.tbl; i++) {
					dependent = wrl.schema.get(tbl.getTbl_id()).get(i-1); // ArrayList index starts from 0					
					
					if(dependent == 1)
						tbl.getTbl_foreign_tables().add(i);				
				}
			}
		}		
	}
	
	// Populate Database with Data tuples
	public void populate(Workload wrl) {
		
		// Estimate initial table sizes
		this.estimateTableSize((TpccWorkload)wrl);
		
		// Populate initial database
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Populating initial database ...");
		
		for(Entry<Integer, Table> tbl_entry : this.getDb_tables().entrySet()) {
			
			Table tbl = tbl_entry.getValue();
			
			Table ftbl = null;
			Iterator<Integer> f_tbl_itr = null;
			ArrayList<Integer> fk_list = null;
			
			int f_tuple[] = new int[2];
			int i = 0;
			int pk, fk = 0;
						
			for(pk = 1; pk <= tbl.getTbl_init_tuples(); pk++) {
				
				++Global.global_tupleSeq;											
				Tuple tpl = this.insertTuple(tbl.getTbl_id(), pk);
				
				if(tbl.getTbl_type() != 0) {
					
					fk_list = new ArrayList<Integer>();
					f_tbl_itr = tbl.getTbl_foreign_tables().iterator();
					
					while(f_tbl_itr.hasNext()) {
						ftbl = this.getTable(f_tbl_itr.next());
						
						++f_tuple[i];
						fk = ftbl.getTupleByPk(f_tuple[i]).getTuple_pk();
						tpl.getTuple_fk().put(tbl.getTbl_id(), fk);									
						
						// Index
						switch(tbl.getTbl_type()) {
							case 1: // Secondary Table
								// Insert into Index
								tbl.insertSecondaryIdx(fk, pk);									
								break;
							
							case 2:	// Dependent Table								
								fk_list.add(fk);
								break;
						}
						
						// Reset
						if(f_tuple[i] == ftbl.getTbl_tuples().size())
							f_tuple[i] = 0;						

						++i;
					} //--end while()--foreign tables
					
					// Index -- Dependent Table
					if(tbl.getTbl_type() == 2) {
						tbl.getIdx_multikey_dependent().put(fk_list.get(0), fk_list.get(1), pk);
					}
					
					// Reset
					i = 0;
				} //--end if()--secondary and dependent tables
			} //--end for()--data
			
			tbl.setTbl_last_entry(pk);
			Global.LOGGER.info(tbl.getTbl_tuples().size()+" tuples have successfully inserted into "+tbl.getTbl_name()+" table.");			
		} //--end for()--table
		
		// Update tuple counts after initial population
		this.updateTupleCounts();		
		
		// Populate initial database cache														
		this.setupDbCache((TpccWorkload)wrl);
	}
	
	// Setting up TPCC database cache
	private void setupDbCache(TpccWorkload wrl) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Setting up database cache ...");		

		wrl.warmingup = true;
		
		// i -- Transaction types
		for(int i = 1; i <= wrl.tr_types; i++) {
			// Calculate the number of transactions to be created for a specific type
			int tr_nums = (int) Math.ceil(wrl.tr_proportions.get(i) * Global.observationWindow); // 3600 transactions ~ 1hr workload			
			//Global.LOGGER.info("Streaming "+tr_nums+" transactions of type "+i+" ...");

			// j -- number of Transactions for a specific Transaction type
			for(int j = 1; j <= tr_nums; j++) {		
				wrl.getTrTupleSet(this, i);
			}			
		}
		
		wrl.warmingup = false;
		this.updateTupleCounts();
		
		Global.LOGGER.info("After initial population and cache setup, total db tuples are "+this.getDb_tuple_counts()+".");
	}
}