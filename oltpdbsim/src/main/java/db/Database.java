package main.java.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import main.java.entry.Global;
import main.java.utils.Utility;

public class Database implements java.io.Serializable {	

	private static final long serialVersionUID = 4457221663737113018L;

	private String db_name;
	private int db_tuple_counts;
	private SortedMap<Integer, Table> db_tables;
	
	Database(String name) {
		this.setDb_name(name);
		this.setDb_tuple_counts(0);
		this.setDb_tables(new TreeMap<Integer, Table>());
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

	public SortedMap<Integer, Table> getDb_tables() {
		return this.db_tables;
	}

	public void setDb_tables(SortedMap<Integer, Table> db_tables) {
		this.db_tables = db_tables;
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
	
	int getBase(int x) { 
		int value = Math.abs(x);
		
		if (value == 0) 
			return 1; 
		else 
			return (int) (1 + Math.floor((Math.log(value)/Math.log(10.0d)))); 
	}		
	
	// Populate Database with Data tuples
	public void populate() {
		
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
					if(tbl.getTbl_type() == 2){
						tbl.getIdx_multikey_dependent().put(fk_list.get(0), fk_list.get(1), pk);
					}
					
					// Reset
					i = 0;
				} //--end if()--secondary and dependent tables
			} //--end for()--data
			
			tbl.setTbl_last_entry(pk);
			Global.LOGGER.info(tbl.getTbl_tuples().size()+" tuples have successfully inserted into "+tbl.getTbl_name()+" table.");
		} //--end for()--table
		
		this.updateTupleCounts();
		Global.LOGGER.info("After initial population, total tuples: "+this.getDb_tuple_counts());		
	}
}