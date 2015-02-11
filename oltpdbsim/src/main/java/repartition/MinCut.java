package main.java.repartition;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import main.java.entry.Global;
import main.java.utils.Utility;
import main.java.workload.WorkloadBatch;

public class MinCut {

	// gpmets GraphFile Nparts
	// khmetis HGraphFile Nparts UBfactor Nruns CType OType Vcycle dbglvl
		
	private static ArrayList<String> getArgList(WorkloadBatch wb, int partitions) {
		ArrayList<String> arg_list = new ArrayList<String>();
		
		switch(Global.workloadRepresentation) {
			case "gr":
				
				// Graph clustering parameters				
				if(Utility.isWindows()) {
		        	arg_list.add("cmd");
					arg_list.add("/c");
		        	arg_list.add(Global.metis_dir+Global.metis_gr_exec);
		        }
		        
		        if(Utility.isUnix()) {		        	
		        	arg_list.add(Global.metis_dir+Global.metis_gr_exec);
		        }
		        
		        arg_list.add(wb.getWrl_file_name());				// Workload file
				arg_list.add(Integer.toString(partitions));			// Nparts	        	
	        	
				break;
				 
			default: // "hgr" and "chg"
				
				// Hypergraph clustering parameters
		        if(Utility.isWindows()) {
		        	arg_list.add("cmd");
					arg_list.add("/c");
		        	arg_list.add(Global.metis_dir+Global.metis_hgr_exec);
		        }
		        
		        if(Utility.isUnix()) {
		        	arg_list.add(Global.metis_dir+Global.metis_hgr_exec);
		        }
		        
				arg_list.add(wb.getWrl_file_name());				// Workload file
				//arg_list.add(wb.getWrl_fixfile_name());			// Fixfile
				arg_list.add(Integer.toString(partitions));			// Nparts
				arg_list.add("5");									// UBfactor(>=5)
				arg_list.add("20");									// Nruns(>=1)
				arg_list.add("1");									// CType(1-5)
				arg_list.add("1");									// OType(1-2) -- 1: Minimizes the hyper edge cut, 2: Minimizes the sum of external degrees (SOED)
				arg_list.add("0");									// Vcycle(0-3)
				arg_list.add("0");									// dbglvl(0+1+2+4+8+16)
				
				break;
		}
		
		return arg_list;
	}
	
	public static void runMinCut(WorkloadBatch wb, int partitions, boolean waitForResponse) {
		
		long startTime = 0;
		long duration = 0;
		String response = "";
		
		startTime = System.currentTimeMillis();
		Global.LOGGER.info("Clustering workload file ...");
		
		//Console output
		//final PrintStream _ps_console = System.out;
		
		// Get corresponding argument list
		ArrayList<String> arg_list = getArgList(wb, partitions);
		
		// Create a process builder
		String[] arg = arg_list.toArray(new String[arg_list.size()]);					
		ProcessBuilder pb = new ProcessBuilder(arg);
		//final Process p = null;
		
		//System.setOut(_ps_console);
        					
		try {
			final Process p = pb.start();
			//p.waitFor();
			
//			new Thread(new Runnable(){
//				public void run(){
//		        	Scanner stdin = new Scanner(p.getInputStream());
//		        	Scanner stderr = new Scanner(p.getErrorStream());
//		        	
//		            while(stdin.hasNextLine()){
//		            	Global.LOGGER.info(stdin.nextLine());
//		            }
//		            
//		            while(stderr.hasNextLine()){
//		            	Global.LOGGER.info(stderr.nextLine());
//		            }
//		            
//		            stdin.close();
//		            stderr.close();
//				}
//		    }).start();
			
			
			if (waitForResponse) {
				 
				// To capture output from the shell
				InputStream shellIn = p.getInputStream();
				 
				// Wait for the shell to finish and get the return code
				int shellExitStatus = p.waitFor();
				Global.LOGGER.info("Completing workload clustering ... [Exit status (" + shellExitStatus+")]");
				 
				response = convertStreamToStr(shellIn);
				 
				shellIn.close();
			}
			
		} catch (IOException | InterruptedException e) {						
			e.printStackTrace();
		}
		
		Global.LOGGER.info(response);
		
		duration = (System.currentTimeMillis() - startTime);
		Global.LOGGER.info("Workload file has been clustered in "+duration+ " ms.");
		//System.setOut(_ps_console);
	}
	
	/*
	* To convert the InputStream to String we use the Reader.read(char[]
	* buffer) method. We iterate until the Reader return -1 which means
	* there's no more data to read. We use the StringWriter class to
	* produce the string.
	*/
	 
	public static String convertStreamToStr(InputStream is) throws IOException {
	 
		if (is != null) {
			
			Writer writer = new StringWriter();			 
			char[] buffer = new char[1024];
			
			try {
				Reader reader = new BufferedReader(new InputStreamReader(is, "UTF-8"));
				
				int n;
				while ((n = reader.read(buffer)) != -1) {
					writer.write(buffer, 0, n);
				}
			} finally {
				is.close();
			}
			
			return writer.toString();
			
		} else {
			return "";
		}
	}
}