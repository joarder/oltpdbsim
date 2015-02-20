package main.java.db;

import org.apache.commons.math3.distribution.ZipfDistribution;

import main.java.entry.Global;
import main.java.workload.TpccWorkload;

public class TpccDatabase extends Database {

	private static final long serialVersionUID = -2439483630692483461L;

	public TpccDatabase(String name) {
		super(name);
	}
		
	public void estimateTableSize(TpccWorkload tpcc) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Estimating initial data tuple counts for "+this.getDb_name()+" database ...");
		
		String[] table_names = {"Warehouse", "Item", "District", "Stock", "Customer", "History", "Orders", "New-Order", "Order-Line"};
		int dependent = 0;
		
		for(int tbl_id = 1; tbl_id <= tpcc.tbl; tbl_id++) {
			Table tbl = new Table(tbl_id, tpcc.tbl_types.get(tbl_id), table_names[tbl_id-1]);			
			
			// Set TPCC table ranks for Warehouse and Item tables
			if(tbl.getTbl_id() == 1) {
				tbl.setTbl_data_rank(new int[tpcc.warehouses + 1]);
				//new
				tbl.zipfDistribution = new ZipfDistribution(tpcc.warehouses, 2.0); // exponent = 2.0
				tbl.zipfDistribution.reseedRandomGenerator(Global.repeated_runs);
				
			} else if(tbl.getTbl_id() == 2) {
				tbl.setTbl_data_rank(new int[((int) (100000 * tpcc.scale)) + 1]);
				//new
				tbl.zipfDistribution = new ZipfDistribution(((int) (100000 * tpcc.scale)), 2.0); // exponent = 2.0
				tbl.zipfDistribution.reseedRandomGenerator(Global.repeated_runs);				
			}
			
			this.getDb_tables().put(tbl_id, tbl);			
			
			// Determine the number of Data rows to be populated for each individual table
			switch(tbl.getTbl_name()) {
				case "Warehouse":
					tbl.setTbl_init_tuples(tpcc.warehouses);
					break;
					
				case "Item":
					tbl.setTbl_init_tuples((int) (100000 * tpcc.scale));					
					break;
					
				case "District":
					tbl.setTbl_init_tuples(10 * tpcc.warehouses);
					break;
					
				case "Stock":
					tbl.setTbl_init_tuples((int) (100000 * tpcc.warehouses * tpcc.scale));
					break;
					
				case "Customer": // District Table
					tbl.setTbl_init_tuples((int) (3000 * this.getTable(3).getTbl_init_tuples() * tpcc.scale));
					break;
					
				case "History": // Customer Table
					tbl.setTbl_init_tuples(this.getTable(4).getTbl_init_tuples()); 
					break;
					
				case "Orders":
					tbl.setTbl_init_tuples((int) (30000 * tpcc.warehouses * tpcc.scale));
					break;
					
				case "New-Order": // Customer Table (the last 1/3 values from the Order table)					
					tbl.setTbl_init_tuples((int) (Math.pow(10.0d, (getBase(this.getTable(6).getTbl_init_tuples()) -1)) 
									- Math.pow(10.0d, (getBase(this.getTable(6).getTbl_init_tuples()) -2))));					
					break;
				case "Order-Line": // Stock Table (10 most popular values from the Stock table)
					tbl.setTbl_init_tuples(this.getTable(6).getTbl_init_tuples() * 10);
					break;
			}
			
			Global.LOGGER.info(tbl.getTbl_name()+": "+tbl.getTbl_init_tuples());
			
			// Setting dependency information for the database tables
			if(tbl.getTbl_type() != 0){
				for(int i = 1; i <= tpcc.tbl; i++) {
					dependent = tpcc.schema.get(tbl.getTbl_id()).get(i-1); // ArrayList index starts from 0					
					
					if(dependent == 1)
						tbl.getTbl_foreign_tables().add(i);				
				}
			}
		}		
	}
}