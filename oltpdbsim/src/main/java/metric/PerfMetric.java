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

package main.java.metric;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import main.java.entry.Global;
import main.java.utils.Utility;

public class PerfMetric {	

	// For all transactions | perf1
	public Map<Integer, Double> time;
	public Map<Integer, Double> I_Dt;
	public Map<Integer, Integer> Unqlen;
	
	// For unique transactions | perf2
	public Map<Integer, Double> Period;
	public Map<Integer, Integer> Span;
	public Map<Integer, Double> Response;
	
	private static File file1;
	private static File file2;
	public static File file3;
	
	private PrintWriter prWriter1;
	private PrintWriter prWriter2;
	public PrintWriter prWriter3;
	
	public PerfMetric() {
		time = new HashMap<Integer, Double>();
		I_Dt = new HashMap<Integer, Double>();
		Unqlen = new HashMap<Integer, Integer>();
		
		// For unique transactions | perf2
		Period = new HashMap<Integer, Double>(); // Holds inter-repetition intervals
		Span = new HashMap<Integer, Integer>();
		Response = new  HashMap<Integer, Double>();
		
		// Creating a metric files
		file1 = new File(Global.metric_dir+"run"+Global.repeated_runs+"/"
				+Global.simulation+"-s"+Global.servers+"-p"+Global.partitions+"-perf1.out");
		
		file2 = new File(Global.metric_dir+"run"+Global.repeated_runs+"/"
				+Global.simulation+"-s"+Global.servers+"-p"+Global.partitions+"-perf2.out");
		
		if(Global.analysis)
			file3 = new File(Global.metric_dir+"run"+Global.repeated_runs+"/"
				+Global.simulation+"-s"+Global.servers+"-p"+Global.partitions+"-perf3.out");
		
		/*try {			
			file1.getParentFile().mkdirs();
			file2.getParentFile().mkdirs();
			
			if(Global.analysis)
				file3.getParentFile().mkdirs();
			
			if(file1.exists())
				file1.delete();
			
			file1.createNewFile();
			
			if(file2.exists())
				file2.delete();
			
			file2.createNewFile();
			
			if(Global.analysis) {
				if(file3.exists())
					file3.delete();
				
				file3.createNewFile();
			}			
		} catch (IOException e) {
			Global.LOGGER.error("Failed in creating metric directory or file !!", e);
		}*/
		
		// File Writers
		prWriter1 = Utility.getPrintWriter(Global.metric_dir, file1);
		prWriter2 = Utility.getPrintWriter(Global.metric_dir, file2);
		
		if(Global.analysis)
			prWriter3 = Utility.getPrintWriter(Global.metric_dir, file3);
	}
	
	public void write() {
		
		// Write perf1		
		try {
			for(Entry<Integer, Double> entry : time.entrySet()) {
				prWriter1.print(entry.getValue()+" ");
				prWriter1.print(I_Dt.get(entry.getKey())+" ");
				prWriter1.print(Unqlen.get(entry.getKey()));
				prWriter1.println();
			}
		} finally {
			prWriter1.close();
		}
		
		// Write perf2		
		try {
			for(Entry<Integer, Double> entry : Period.entrySet()) {
				prWriter2.print(entry.getValue()+" ");
				prWriter2.print(Span.get(entry.getKey())+" ");
				//prWriter2.print(Response.get(entry.getKey()));
				prWriter2.println();
			}
		} finally {
			prWriter2.close();
		}
		
		if(Global.analysis)
			prWriter3.close();
	}
}