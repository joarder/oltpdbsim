package main.java.db;

import java.util.HashMap;
import java.util.Map;
import main.java.utils.Utility;

public class Tuple implements Comparable<Tuple>, java.io.Serializable {
	
	private static final long serialVersionUID = -1372118580750617923L;
	
	private int tuple_id;
	private int tuple_pk;
	private String tuple_label;
	private String tuple_action;
	private Map<Integer, Integer> tuple_fk;
	
	Tuple(Table tbl, int pk) {
		this.setTuple_id(Utility.rightPadding(pk, tbl.getTbl_id()));
		this.setTuple_label("t"+this.getTuple_id());
		this.setTuple_action("initial");
		
		switch(tbl.getTbl_type()) {
			case 0: // Primary Table (i.e. Warehouse and Item Tables in TPCC)
				this.setTuple_pk(pk);
				break;
				
			case 1: // Secondary Table
				this.setTuple_pk(pk);
				this.setTuple_fk(new HashMap<Integer, Integer>());
				break;
				
			case 2: // Dependent Table (i.e. History Table in TPCC)
				this.setTuple_fk(new HashMap<Integer, Integer>());
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

	public Map<Integer, Integer> getTuple_fk() {
		return tuple_fk;
	}

	public void setTuple_fk(Map<Integer, Integer> tuple_fk) {
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