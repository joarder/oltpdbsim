package main.java.entry;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import main.java.db.Database;
import main.java.workload.Workload;

public final class Serialize {
	
	// File I/O
	static File dbFile; //, wrlFile = null;
	static FileOutputStream dbOutStream = null; //, wrlOutStream = null;
	static ObjectOutputStream dbOut = null; //, wrlOut = null;
	
	public static void execute(Database db, Workload wrl) {
		// Serialize Database and Workload objects
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Serializing database, workloads and global parameters for run "+Global.repeated_runs+" ...");
		
		try{
			Global.ext = Global.repeated_runs+".ser";
			
			dbFile = new File(Global.wrl_dir+"/"+"run"+Global.repeated_runs+"/"+"db"+Global.ext);
			//wrlFile = new File(Global.dir+"/"+"run"+Global.repeated_runs+"/"+"wrl"+Global.ext);
			
			dbFile.getParentFile().mkdirs();				
			dbFile.createNewFile();
			
			//wrlFile.getParentFile().mkdirs();
			//wrlFile.createNewFile();									

			dbOutStream = new FileOutputStream(dbFile);
			//wrlOutStream = new FileOutputStream(wrlFile);
			
			dbOut = new ObjectOutputStream(dbOutStream);
			//wrlOut = new ObjectOutputStream(wrlOutStream);
			
			dbOut.writeObject(db);
			//wrlOut.writeObject(wrl);
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {			
			try {
				dbOutStream.close();
				//wrlOutStream.close();
				
				dbOut.close();
				//wrlOut.close();
			} catch (IOException e) {				
				e.printStackTrace();
			}			
		}
		
		Global.LOGGER.info("Serialization process has successfully completed.");
	}
}