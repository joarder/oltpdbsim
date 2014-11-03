package main.java.entry;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import main.java.db.Database;
import main.java.workload.Workload;

public final class Deserialize {

	// File I/O
	static File dbFile; //, wrlFile = null;
	static FileInputStream dbInStream = null; //, wrlInStream = null;
	static ObjectInputStream dbIn = null; //, wrlIn = null;
	
	public static Database execute(Database db, Workload wrl) { 
		
		Global.LOGGER.info("Deserializing database, workloads and global parameters ...");
		
		try {
			Global.ext = Global.repeated_runs+".ser";
			
			dbFile = new File(Global.wrl_dir+"/"+"run"+Global.repeated_runs+"/"+"db"+Global.ext);
			//wrlFile = new File(Global.dir+"/"+"run"+Global.repeated_runs+"/"+"wrl"+Global.ext);
			
			dbInStream = new FileInputStream(dbFile);
			//wrlInStream = new FileInputStream(wrlFile);
			
			dbIn = new ObjectInputStream(dbInStream);
			//wrlIn = new ObjectInputStream(wrlInStream);
			
			db = (Database) dbIn.readObject();
			//wrl = (Workload) wrlIn.readObject();
			
		} catch(IOException | ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			try {
				dbIn.close();
				//wrlIn.close();
				
				dbInStream.close();
				//wrlInStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
		
		Global.LOGGER.info("Target database: "+db.getDb_name());
		Global.LOGGER.info("Total tuple counts from database: "+db.getDb_tuple_counts());
		
		Global.LOGGER.info("Deserialization process is successfully completed.");		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		
		return db;
	}
}