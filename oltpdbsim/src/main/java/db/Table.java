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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import main.java.entry.Global;
import main.java.utils.Utility;

import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.math3.distribution.ZipfDistribution;

public class Table implements Comparable<Table> {
	
	private int tbl_id;
	private String tbl_name;
	private int tbl_type;
	private int tbl_init_tuples;
	private Map<Integer, Tuple> tbl_tuples; // <id, Tuple> pairs
	private Set<Integer> tbl_foreign_tables;
	private int[] tbl_tuple_rank;
	private int tbl_last_entry;
	public ZipfDistribution zipfDistribution;
	
	// Indexes
	private Map<Integer, Set<Integer>> idx_multivalue_secondary; // For Secondary Tables
	private MultiKeyMap<Integer, Integer> idx_multikey_dependent; // For Dependent Tables
	
	public Table(int id, int type, String name) {
		this.setTbl_id(id);
		this.setTbl_name(name);
		this.setTbl_type(type);
		this.setTbl_init_tuples(0);
		this.setTbl_tuples(new HashMap<Integer, Tuple>());
		this.setTbl_last_entry(0);
		
		switch(this.getTbl_type()){
		case 0:	// Primary Table (i.e. Warehouse and Item Tables in TPCC)
			break;
			
		case 1: // Secondary Table
			this.setTbl_foreign_tables(new HashSet<Integer>());
			this.setIdx_multivalue_secondary(new HashMap<Integer, Set<Integer>>());
			break;
			
		case 2: // Dependent Table (i.e. History Table in TPCC)
			this.setTbl_foreign_tables(new HashSet<Integer>());
			this.setIdx_multikey_dependent(new MultiKeyMap<Integer, Integer>());
			break;
		}
	}
	
	public int getTbl_id() {
		return tbl_id;
	}

	public void setTbl_id(int tbl_id) {
		this.tbl_id = tbl_id;
	}

	public String getTbl_name() {
		return tbl_name;
	}

	public void setTbl_name(String tbl_name) {
		this.tbl_name = tbl_name;
	}

	public int getTbl_type() {
		return tbl_type;
	}

	public void setTbl_type(int tbl_type) {
		this.tbl_type = tbl_type;
	}
	
	public int getTbl_init_tuples() {
		return tbl_init_tuples;
	}

	public void setTbl_init_tuples(int tbl_init_tuples) {
		this.tbl_init_tuples = tbl_init_tuples;
	}

	public Map<Integer, Tuple> getTbl_tuples() {
		return tbl_tuples;
	}

	public void setTbl_tuples(Map<Integer, Tuple> tbl_tuples) {
		this.tbl_tuples = tbl_tuples;
	}

	public Set<Integer> getTbl_foreign_tables() {
		return tbl_foreign_tables;
	}

	public void setTbl_foreign_tables(Set<Integer> tbl_foreign_tables) {
		this.tbl_foreign_tables = tbl_foreign_tables;
	}

	public Map<Integer, Set<Integer>> getIdx_multivalue_secondary() {
		return idx_multivalue_secondary;
	}

	public void setIdx_multivalue_secondary(Map<Integer, Set<Integer>> tbl_tuple_map_s) {
		this.idx_multivalue_secondary = tbl_tuple_map_s;
	}

	public MultiKeyMap<Integer, Integer> getIdx_multikey_dependent() {
		return idx_multikey_dependent;
	}

	public void setIdx_multikey_dependent(MultiKeyMap<Integer, Integer> tbl_tuple_map_d) {
		this.idx_multikey_dependent = tbl_tuple_map_d;
	}

	public int[] getTbl_dataRank() {
		return tbl_tuple_rank;
	}

	public void setTbl_data_rank(int[] tbl_data_rank) {
		this.tbl_tuple_rank = tbl_data_rank;
	}

	public int getTbl_last_entry() {
		return tbl_last_entry;
	}

	public void setTbl_last_entry(int tbl_last_entry) {
		this.tbl_last_entry = tbl_last_entry;
	}
	
	// Returns a Tuple by its primary key
	public Tuple getTupleByPk(int pk) {
		int tpl_id = Utility.rightPadding(pk, this.getTbl_id());
		return this.getTbl_tuples().get(tpl_id);
	}
	
	// Insert into Index
	public void insertSecondaryIdx(int pk, int fk) {		
		if(this.getIdx_multivalue_secondary().containsKey(pk))
			this.getIdx_multivalue_secondary().get(pk).add(fk);
		else {
			Set<Integer> fkeySet = new HashSet<Integer>();
			fkeySet.add(fk);
			this.getIdx_multivalue_secondary().put(pk, fkeySet);
		}
	}
	
	public ArrayList<Integer> getTableData(ArrayList<Integer> keyList) {
		ArrayList<Integer> dataList = new ArrayList<Integer>();
		ArrayList<Integer> tpl_id = new ArrayList<Integer>();
		boolean _null = false;
		int rand = 0;		
		int d = -1;				
		
		switch(this.getTbl_type()) {
			case 1:
				for(Integer i : keyList)
					dataList.addAll(this.getIdx_multivalue_secondary().get(i));			
								
				if(dataList.size() > 1) {
					rand = Global.rand.nextInt(dataList.size());					
					d = dataList.get(rand);					
				} else
					d = dataList.get(0);				
				
				break;
				
			case 2:
				if(this.getIdx_multikey_dependent().get(keyList.get(0), keyList.get(1)) == null)
					_null = true;
				else
					d = this.getIdx_multikey_dependent().get(keyList.get(0), keyList.get(1));
				
				break;
		}
		
		if(_null)
			tpl_id.add(-1);
		else {
			//System.out.println("@ d = "+d+" | "+this.getTbl_type());
			//System.out.println("@ "+this.getTupleByPk(d).toString());
			if(this.getTupleByPk(d).getTuple_pk() != 0)
				tpl_id.add(this.getTupleByPk(d).getTuple_pk());
			
			//System.out.println("@ "+tpl_id);
		}
		
		tpl_id.add(d);
		//System.out.println("@ "+tpl_id);
		
		return tpl_id;
	}
	
	@Override
	public boolean equals(Object tbl) {
		if (!(tbl instanceof Table)) {
			return false;
		}
		
		Table table = (Table) tbl;
		return (this.getTbl_name().equals(table.getTbl_name()));
	}

	@Override
	public int hashCode() {
		return (this.getTbl_name().hashCode());
	}

	@Override
	public int compareTo(Table table) {		
		return (((int)this.getTbl_id() < (int)table.getTbl_id()) ? -1: 
			((int)this.getTbl_id() > (int)table.getTbl_id()) ? 1:0);		
	}
	
	@Override
	public String toString() {
		return ("Table("+this.getTbl_name()+") | Tuples["+this.getTbl_tuples().size()+"]");
	}
}