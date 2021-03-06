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
import java.util.Map;
import java.util.Set;

import main.java.utils.Utility;
import main.java.workload.WorkloadConstants;

public class Tuple implements Comparable<Tuple> {
	
	private int tuple_id;
	private int tuple_pk;
	private String tuple_label;
	private String tuple_action;
	private Map<Integer, Set<Integer>> tuple_fk;
	
	Tuple(Table tbl, int pk) {
		this.setTuple_id(Utility.rightPadding(pk, tbl.getTbl_id()));
		this.setTuple_label("t"+this.getTuple_id());
		this.setTuple_action(WorkloadConstants.TPL_INITIAL);
		
		switch(tbl.getTbl_type()) {
			case 0: // Primary Table (i.e. Warehouse and Item Tables in TPCC)
				this.setTuple_pk(pk);
				break;
				
			case 1: // Secondary Table
				this.setTuple_pk(pk);
				this.setTuple_fk(new HashMap<Integer, Set<Integer>>());
				break;
				
			case 2: // Dependent Table (i.e. History Table in TPCC)
				this.setTuple_fk(new HashMap<Integer, Set<Integer>>());
				break;
		}		
	}
	
	public int getTuple_id() {
		return tuple_id;
	}

	public void setTuple_id(int tuple_id) {
		this.tuple_id = tuple_id;
	}	

	public String getTuple_label() {
		return tuple_label;
	}

	public void setTuple_label(String tuple_label) {
		this.tuple_label = tuple_label;
	}
	
	public String getTuple_action() {
		return tuple_action;
	}

	public void setTuple_action(String tuple_action) {
		this.tuple_action = tuple_action;
	}

	public int getTuple_pk() {
		return tuple_pk;
	}

	public void setTuple_pk(int tuple_pk) {
		this.tuple_pk = tuple_pk;
	}

	public Map<Integer, Set<Integer>> getTuple_fk() {
		return tuple_fk;
	}

	public void setTuple_fk(Map<Integer, Set<Integer>> tuple_fk) {
		this.tuple_fk = tuple_fk;
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Tuple)) {
			return false;
		}
		
		Tuple tpl = (Tuple) object;
		return this.getTuple_label().equals(tpl.getTuple_label());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + tuple_id;
		return result;
	}

	@Override
	public int compareTo(Tuple tuple) {		
		return (((int)this.getTuple_pk() < (int)tuple.getTuple_pk()) ? -1 : 
			((int)this.getTuple_pk() > (int)tuple.getTuple_pk()) ? 1:0);		
	}
	
	@Override
	public String toString() {
		return ("Tuple("+this.getTuple_id()+") | PK["+this.getTuple_pk()+"] | Action["+this.getTuple_action()+"]");
	}
}