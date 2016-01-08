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
	private static ArrayList<String> getArgList(WorkloadBatch wb, int partitions) {
		ArrayList<String> arg_list = new ArrayList<String>();
		
		switch(Global.workloadRepresentation) {
			case "gr": // GR and CGR				
				// Graph clustering parameters
				// gpmets GraphFile Nparts
				if(Utility.isWindows()) {
		        	arg_list.add("cmd");
					arg_list.add("/c");
				}
		        
				arg_list.add(Global.metis_dir+Global.metis_gr_exec);		        
		        arg_list.add(wb.getWrl_file_name());				// Workload file
				arg_list.add(Integer.toString(partitions));			// Nparts
				
				//if(Global.compressionEnabled)
					//arg_list.add("-ptype=rb");					// ptype -- Multilevel recursive bisectioning. Default is kway -- Multilevel k-way partitioning	
	        	
				break;
				 
			default: // HR and CHR				
				// Hypergraph clustering parameters
				// khmetis HGraphFile Nparts UBfactor Nruns CType OType Vcycle dbglvl
		        if(Utility.isWindows()) {
		        	arg_list.add("cmd");
					arg_list.add("/c");
		        }
		        
		        arg_list.add(Global.metis_dir+Global.metis_hgr_exec);
				arg_list.add(wb.getWrl_file_name());				// Workload file
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
		long startTime = System.currentTimeMillis();
		long duration = 0;
		String response = "";
		
		Global.LOGGER.info("Clustering workload file ...");
		
		// Get corresponding argument list
		ArrayList<String> arg_list = getArgList(wb, partitions);
		
		// Create a process builder
		String[] arg = arg_list.toArray(new String[arg_list.size()]);					
		ProcessBuilder pb = new ProcessBuilder(arg);

		try {
			final Process p = pb.start();
			
			if (waitForResponse) {				 
				// To capture output from the shell
				InputStream shellIn = p.getInputStream();
				 
				// Wait for the shell to finish and get the return code
				int shellExitStatus = p.waitFor();
				Global.LOGGER.info("Completing workload clustering ... [Exit status ("+shellExitStatus+")]");
				 
				response = convertStreamToStr(shellIn);				 
				shellIn.close();
			}			
		} catch (IOException | InterruptedException e) {						
			e.printStackTrace();
		}
		
		Global.LOGGER.info(response);
		
		duration = (System.currentTimeMillis() - startTime);
		Global.LOGGER.info("Workload file has been clustered in "+duration+ " ms.");
	}
	
	/*
	* To convert the InputStream to String we use the Reader.read(char[]
	* buffer) method. We iterate until the Reader return -1 which means
	* there's no more data to read. We use the StringWriter class to
	* produce the string.
	*/
	 
	static String convertStreamToStr(InputStream is) throws IOException {	 
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