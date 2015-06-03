package main.java.workload;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import main.java.cluster.Cluster;
import main.java.cluster.Data;

public class Transaction implements Comparable<Transaction>, java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	private int tr_id;	
	private String tr_label;
	private int tr_type;
	private String tr_class;
	
	private double tr_period;
	
	private Set<Integer> tr_dataSet;	
	private HashMap<Integer, HashSet<Integer>> tr_partitionSet;
	private HashMap<Integer, HashSet<Integer>> tr_serverSet;
	
	private int tr_ssCost; // Server Span Cost or, Distributed Transaction Cost
	private int tr_psCost; // Partition Span Cost	
	
	private boolean processed;	
	private boolean dt;
	private boolean span2Servers;
	
	private double tr_arrival_time;
	private double tr_service_time;
	private double tr_response_time;
	private double tr_waiting_time;	
	
	private double tr_idt;
	
	Transaction(int id, Set<Integer> dataSet) {		
		this.setTr_id(id);
		this.setTr_label("T"+Integer.toString(this.getTr_id()));
		this.setTr_type(0);
		this.setTr_dataSet(dataSet);
	}
	
	public Transaction(int id, int type, Set<Integer> dataSet, double time) {
		this.setTr_id(id);
		this.setTr_label("T"+Integer.toString(this.getTr_id()));
		this.setTr_type(type);
		this.setTr_dataSet(dataSet);
				
		this.setTr_serverSpanCost(0);
		this.setTr_partitionSpanCost(0);

		this.setProcessed(false);
		this.setDt(false);
		this.setSpan2Servers(false);
		
		this.setTr_arrival_time(0.0);
		this.setTr_service_time(0.0);
		this.setTr_response_time(0.0);
		
		this.setTr_partitionSet(new HashMap<Integer, HashSet<Integer>>());
		this.setTr_serverSet(new HashMap<Integer, HashSet<Integer>>());
		
		this.setTr_class(null);		
		this.setTr_period(0.0);
		
		this.setTr_idt(0.0d);
	}
	
	public int getTr_id() {
		return tr_id;
	}

	public void setTr_id(int tr_id) {
		this.tr_id = tr_id;
	}

	public int getTr_type() {
		return tr_type;
	}

	public void setTr_type(int type) {
		this.tr_type = type;
	}
	
	public String getTr_label() {
		return tr_label;
	}
	
	public void setTr_label(String tr_label) {
		this.tr_label = tr_label;
	}
	
	public int getTr_serverSpanCost() {
		return tr_ssCost;
	}

	public void setTr_serverSpanCost(int tr_ssCost) {
		this.tr_ssCost = tr_ssCost;
	}

	public int getTr_partitionSpanCost() {
		return tr_psCost;
	}

	public void setTr_partitionSpanCost(int tr_psCost) {
		this.tr_psCost = tr_psCost;
	}
	
	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

	public boolean isDt() {
		return dt;
	}

	public void setDt(boolean dt) {
		this.dt = dt;
	}

	public boolean isSpans2Servers() {
		return span2Servers;
	}

	public void setSpan2Servers(boolean span2Servers) {
		this.span2Servers = span2Servers;
	}

	public double getTr_arrival_time() {
		return tr_arrival_time;
	}

	public void setTr_arrival_time(double tr_arrival_time) {
		this.tr_arrival_time = tr_arrival_time;
	}

	public double getTr_service_time() {
		return tr_service_time;
	}

	public void setTr_service_time(double tr_service_time) {
		this.tr_service_time = tr_service_time;
	}

	public double getTr_response_time() {
		return tr_response_time;
	}

	public void setTr_response_time(double tr_response_time) {
		this.tr_response_time = tr_response_time;
	}

	public double getTr_waiting_time() {
		return tr_waiting_time;
	}

	public void setTr_waiting_time(double tr_waiting_time) {
		this.tr_waiting_time = tr_waiting_time;
	}

	public Set<Integer> getTr_dataSet() {
		return tr_dataSet;
	}

	public void setTr_dataSet(Set<Integer> tr_dataSet) {
		this.tr_dataSet = tr_dataSet;
	}
	
	
	public HashMap<Integer, HashSet<Integer>> getTr_partitionSet() {
		return tr_partitionSet;
	}

	public void setTr_partitionSet(HashMap<Integer, HashSet<Integer>> hashMap) {
		this.tr_partitionSet = hashMap;
	}

	public HashMap<Integer, HashSet<Integer>> getTr_serverSet() {
		return tr_serverSet;
	}

	public void setTr_serverSet(HashMap<Integer, HashSet<Integer>> tr_serverSet) {
		this.tr_serverSet = tr_serverSet;
	}

	public String getTr_class() {
		return tr_class;
	}

	public void setTr_class(String tr_class) {
		this.tr_class = tr_class;
	}
	
	public double getTr_period() {
		return tr_period;
	}

	public void setTr_period(double tr_period) {
		this.tr_period = tr_period;
	}
	
	public double getTr_idt() {
		return tr_idt;
	}

	public void setTr_idt(double tr_idt) {
		this.tr_idt = tr_idt;
	}

	// Calculates Idt
	public void calculateIdt() {
		double freq = 1/this.getTr_period();
		double span = this.getTr_serverSpanCost();
		
		this.setTr_idt(freq*span);
	}
	
	// Calculates Idt for a given server span
	public void calculateIdt(int span) {
		this.setTr_idt((1/this.getTr_period()) * span);
	}

	// This function will calculate the Node and Partition Span Cost for the representative Transaction
	public void calculateSpans(Cluster cluster) {

		// Reset
		this.setTr_partitionSet(new HashMap<Integer, HashSet<Integer>>());
		this.setTr_serverSet(new HashMap<Integer, HashSet<Integer>>());
		
		// Calculate Server and Partition span cost which is equivalent to the cost of Distributed Transaction			
		HashSet<Integer> tr_partitions;
		HashSet<Integer> tr_servers;
		
		Iterator<Integer> d = this.getTr_dataSet().iterator();
		while(d.hasNext()) {
			int data_id = d.next();
			Data data = cluster.getData(data_id);
			
			int p_id = data.getData_partition_id();
			int s_id = data.getData_server_id();

			// Partition Set
			if(this.getTr_partitionSet().containsKey(p_id))
				this.getTr_partitionSet().get(p_id).add(data_id);
			else {
				tr_partitions = new HashSet<Integer>();
				tr_partitions.add(data_id);
				this.getTr_partitionSet().put(p_id, tr_partitions);
			}			
			
			// Server Set
			if(this.getTr_serverSet().containsKey(s_id))
				this.getTr_serverSet().get(s_id).add(data_id);
			else {
				tr_servers = new HashSet<Integer>();
				tr_servers.add(data_id);
				this.getTr_serverSet().put(s_id, tr_servers);
			}
		}
				
		this.setTr_partitionSpanCost(0);
		this.setTr_partitionSpanCost(this.getTr_partitionSet().size());
		
		this.setTr_serverSpanCost(0);
		this.setTr_serverSpanCost(this.getTr_serverSet().size());			
		
		if(this.getTr_serverSet().size() > 1)
			this.setDt(true);
		
		if(this.getTr_serverSet().size() == 2) // For Sword and Association
			this.setSpan2Servers(true);						
	}
	
	@Override
	public String toString() {	
		return (this.getTr_label()+"("
				+"IDT["+this.getTr_idt()+"] | "
				+"Type["+this.tr_type+"] | "
				+"DT["+this.isDt()+"] | "				
				+"DS"+this.getTr_dataSet()+" | "
				+"PS"+this.getTr_partitionSet()+" | "
				+"SS"+this.getTr_serverSet()+" | "
				+" RSP["+this.getTr_response_time()+"]"//+"|"
				//+" Arrival = "+this.getTr_arrival_time()+"|"
				//+" Service = "+this.getTr_service_time()
				+")");
	}

	@Override
	public int compareTo(Transaction transaction) {
		int compare = ((int)this.tr_id < (int)transaction.tr_id) ? -1 : 
			((int)this.tr_id > (int)transaction.tr_id) ? 1 : 0;
		
		return compare;
	}
}