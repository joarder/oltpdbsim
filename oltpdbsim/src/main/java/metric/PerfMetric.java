package main.java.metric;

import java.io.File;
import java.io.IOException;
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
	
	private PrintWriter prWriter1;
	private PrintWriter prWriter2;
	
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
		
		try {			
			file1.getParentFile().mkdirs();
			file2.getParentFile().mkdirs();
			
			file1.createNewFile();
			file2.createNewFile();
		} catch (IOException e) {
			Global.LOGGER.error("Failed in creating metric directory or file !!", e);
		}
		
		// File Writers
		prWriter1 = Utility.getPrintWriter(Global.metric_dir, file1);
		prWriter2 = Utility.getPrintWriter(Global.metric_dir, file2);
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
				prWriter2.print(Response.get(entry.getKey()));
				prWriter2.println();
			}
		} finally {
			prWriter2.close();
		}
	}
}