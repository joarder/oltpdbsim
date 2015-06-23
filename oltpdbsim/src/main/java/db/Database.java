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

package main.java.db;

import java.util.HashMap;
import java.util.Map.Entry;

import main.java.utils.Utility;
import main.java.workload.Workload;

public class Database {	

	private String db_name;
	private int db_tuple_counts;
	private HashMap<Integer, Table> db_tables;
	private HashMap<String, Integer> db_tbl_name_id_map;
	
	protected Database(String name) {
		this.setDb_name(name);
		this.setDb_tuple_counts(0);
		this.setDb_tables(new HashMap<Integer, Table>());
		this.setDb_tbl_name_id_map(new HashMap<String, Integer>());
	}

	public String getDb_name() {
		return db_name;
	}

	public void setDb_name(String db_name) {
		this.db_name = db_name;
	}
	
	public int getDb_tuple_counts() {
		return db_tuple_counts;
	}

	public void setDb_tuple_counts(int db_tuple_counts) {
		this.db_tuple_counts = db_tuple_counts;
	}

	public HashMap<Integer, Table> getDb_tables() {
		return this.db_tables;
	}

	public void setDb_tables(HashMap<Integer, Table> db_tables) {
		this.db_tables = db_tables;
	}
		
	public HashMap<String, Integer> getDb_tbl_name_id_map() {
		return db_tbl_name_id_map;
	}

	public void setDb_tbl_name_id_map(HashMap<String, Integer> db_tbl_name_id_map) {
		this.db_tbl_name_id_map = db_tbl_name_id_map;
	}

	// Updates total Tuple counts within the entire Database
	public void updateTupleCounts() {
		int tuple_count = 0;
		
		for(Entry<Integer, Table> tbl_entry : this.getDb_tables().entrySet())
			tuple_count += tbl_entry.getValue().getTbl_tuples().size();
		
		this.setDb_tuple_counts(tuple_count);
	}

	// Creates and returns a new Tuple | Always uses primary key
	public Tuple insertTuple(int tbl_id, int pk) {
		Table tbl = this.getTable(tbl_id);
		
		Tuple tpl = new Tuple(tbl, pk);
		tbl.getTbl_tuples().put(tpl.getTuple_id(), tpl);
		
		return tpl;
	}
	
	// Retrieves a Tuple by its id and Table id 
	public Tuple getTupleById(int tbl_id, int tpl_id) {		
		return this.getTable(tbl_id).getTbl_tuples().get(tpl_id);		
	}
	
	// Retrieves a Tuple by its primary key and Table id
	public Tuple getTupleByPk(int tbl_id, int pk) {
		int tpl_id = Utility.rightPadding(pk, tbl_id);
		return this.getTupleById(tbl_id, tpl_id);
	}

	// Deletes a Tuple by its id
	public void deleteTupleById(int tbl_id, int tpl_id) {
		this.getTable(tbl_id).getTbl_tuples().remove(tpl_id);		
	}

	// Deletes a Tuple by its primary key
	public void deleteTupleByPk(int tbl_id, int pk) {		
		int tpl_id = Utility.rightPadding(pk, tbl_id);
		this.deleteTupleById(tbl_id, tpl_id);		
	}
	
	// Returns a Table by its id
	public Table getTable(int tbl_id) {
		return this.getDb_tables().get(tbl_id);
	}
	
	// Populate initial database
	public void populate(Workload wrl) {}
}