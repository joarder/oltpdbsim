package main.java.workload;

import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import umontreal.iro.lecuyer.simevents.Sim;
import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;

public class Transaction implements Comparable<Transaction>, java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	private int tr_id;	
	private String tr_label;
	private int tr_type;
	private Set<Integer> tr_dataSet;
	
	private int tr_frequency;
	private int tr_temporal_weight;
	private int tr_ssCost; // Server Span Cost or, Distributed Transaction Cost
	private int tr_psCost; // Partition Span Cost
	private int tr_dtImpact;
	
	private boolean visited;
	private boolean processed;
	private boolean temporal;
	private boolean dt;
	private boolean span2Servers;
	
	private double tr_arrival_time;
	private double tr_service_time;
	private double tr_response_time;
	private double tr_waiting_time;	
	
	private Set<Integer> tr_partitionSet;
	private Set<Integer> tr_serverSet;
	private String tr_class;
	
	private double timestamp;
	
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
		
		this.setTr_frequency(1);
		this.setTr_temporal_weight(2);
		
		this.setTr_serverSpanCost(0);
		this.setTr_partitionSpanCost(0);
		
		this.setTr_dtImpact(0);
		
		this.setVisited(false);
		this.setProcessed(false);
		this.setTemporal(false);
		this.setDt(false);
		this.setSpan2Servers(false);
		
		this.setTr_arrival_time(0.0);
		this.setTr_service_time(0.0);
		this.setTr_response_time(0.0);
		
		this.setTr_partitionSet(new TreeSet<Integer>());
		this.setTr_serverSet(new TreeSet<Integer>());
		
		this.setTr_class(null);
		
		this.setTimestamp(time);
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
	
	public int getTr_frequency() {
		return tr_frequency;
	}

	public void setTr_frequency(int tr_frequency) {
		this.tr_frequency = tr_frequency;
	}

	public int getTr_temporal_weight() {
		return tr_temporal_weight;
	}

	public void setTr_temporal_weight(int tr_temporal_weight) {
		this.tr_temporal_weight = tr_temporal_weight;
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

	public int getTr_dtImpact() {
		return tr_dtImpact;
	}

	public void setTr_dtImpact(int tr_dtImpact) {
		this.tr_dtImpact = tr_dtImpact;
	}
	
	public boolean isVisited() {
		return visited;
	}

	public void setVisited(boolean visited) {
		this.visited = visited;
	}

	public boolean isProcessed() {
		return processed;
	}

	public void setProcessed(boolean processed) {
		this.processed = processed;
	}

	public boolean isTemporal() {
		return temporal;
	}

	public void setTemporal(boolean temporal) {
		this.temporal = temporal;
	}

	public boolean isDt() {
		return dt;
	}

	public void setDt(boolean dt) {
		this.dt = dt;
	}

	public boolean isSpan2Servers() {
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
	
	
	public Set<Integer> getTr_partitionSet() {
		return tr_partitionSet;
	}

	public void setTr_partitionSet(Set<Integer> tr_partitionSet) {
		this.tr_partitionSet = tr_partitionSet;
	}

	public Set<Integer> getTr_serverSet() {
		return tr_serverSet;
	}

	public void setTr_serverSet(Set<Integer> tr_serverSet) {
		this.tr_serverSet = tr_serverSet;
	}

	public String getTr_class() {
		return tr_class;
	}

	public void setTr_class(String tr_class) {
		this.tr_class = tr_class;
	}
	
	public double getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(double timestamp) {
		this.timestamp = timestamp;
	}

	public void incTr_frequency() {
		int tr_frequency = this.getTr_frequency();
		this.setTr_frequency(++tr_frequency);
	}
	
	public void decTr_temporalWeight() {
		int tr_temporal_weight = this.getTr_temporal_weight();
		this.setTr_temporal_weight(--tr_temporal_weight);
	}
	
	public boolean isOld() {
		
		if(Sim.time() - this.getTimestamp() > Global.oldTransactionTimestamp) //(3600/Global.workloadChangeProbability))		
			return true;
		else if(this.getTimestamp() >= Integer.MAX_VALUE)
			return true;
		
		return false;
	}
	
	// This function will calculate the Node and Partition Span Cost for the representative Transaction
	public void calculateSpans(Cluster cluster) {

		// Calculate Server and Partition span cost which is equivalent to the cost of Distributed Transaction
		Set<Integer> tr_servers = new TreeSet<Integer>();		
		Set<Integer> tr_partitions = new TreeSet<Integer>();
		
		Iterator<Integer> d = this.getTr_dataSet().iterator();		
		while(d.hasNext()) {
			int data_id = d.next();
			Data data = cluster.getData(data_id);
			
			tr_servers.add(data.getData_server_id());
			tr_partitions.add(data.getData_partition_id());
		}
		
		this.setTr_serverSet(tr_servers);
		this.setTr_serverSpanCost(tr_servers.size());

		this.setTr_partitionSet(tr_partitions);
		this.setTr_partitionSpanCost(tr_partitions.size());	
		
		if(tr_servers.size() > 1)
			this.setDt(true);
		
		if(tr_servers.size() == 2) // Only for Sword
			this.setSpan2Servers(true);						
	}
	
	// Calculate DT Impacts for the Workload
	public void calculateDTImapct() {
		this.setTr_dtImpact(
				this.getTr_serverSpanCost() 
				* this.getTr_frequency() 
				* this.getTr_temporal_weight());
	}
	
	@Override
	public String toString() {	
		return (this.getTr_label()+"("
				+"DT["+this.isDt()+"] | "
				+"SS["+this.getTr_serverSpanCost()+"] | "
				+"FQ["+this.getTr_frequency()+"] | "
				+"TM["+this.getTr_temporal_weight()+"] | "
				+"DS"+this.getTr_dataSet()+" | "
				//+" Processed = "+this.isProcessed()+"|"//processed
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