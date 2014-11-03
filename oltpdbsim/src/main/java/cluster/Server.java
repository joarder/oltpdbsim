package main.java.cluster;

import java.util.Set;
import java.util.TreeSet;

import main.java.entry.Global;

public class Server implements Comparable<Server> {		
	private int server_id;
	private String server_label;	
	private Set<Integer> server_partitions;	
	private double server_load;
	private int server_inflow;
	private int server_outflow;
	private int server_total_data;	
	
	public Server(int id) {
		this.setServer_id(id);
		this.setServer_label("S"+id);		
		this.setServer_partitions(new TreeSet<Integer>());
		this.setServer_load(0.0d);
		this.setServer_inflow(0);
		this.setServer_outflow(0);
		this.setServer_total_data(0);		
	}
	
	public int getServer_id() {
		return server_id;
	}

	public void setServer_id(int server_id) {
		this.server_id = server_id;
	}

	public String getServer_label() {
		return server_label;
	}

	public void setServer_label(String server_label) {
		this.server_label = server_label;
	}

	public double getServer_load() {
		return server_load;
	}

	public void setServer_load(double server_load) {
		this.server_load = server_load;
	}
	
	public void updateServer_load() {
		double load = Math.round((double) this.getServer_total_data()/Global.server_capacity * 100.0) / 100.0;
		this.setServer_load(load);
	}

	public Set<Integer> getServer_partitions() {
		return server_partitions;
	}

	public void setServer_partitions(TreeSet<Integer> treeSet) {
		this.server_partitions = treeSet;
	}
	
	public int getServer_inflow() {
		return server_inflow;
	}

	public void setServer_inflow(int server_inflow) {
		this.server_inflow = server_inflow;
	}

	public int getServer_outflow() {
		return server_outflow;
	}

	public void setServer_outflow(int server_outflow) {
		this.server_outflow = server_outflow;
	}

	public int getServer_total_data() {
		return server_total_data;
	}

	public void setServer_total_data(int server_total_data) {
		this.server_total_data = server_total_data;
	}
	
	public void incServer_totalData(int val){		
		this.setServer_total_data((this.getServer_total_data() + val));
	}
	
	public void decServer_totalData(int val){		
		this.setServer_total_data((this.getServer_total_data() - val));
	}
	
	public void incServer_totalData(){		
		int val = this.getServer_total_data();
		this.setServer_total_data(++val);
	}
	
	public void decServer_totalData(){
		int val = this.getServer_total_data();
		this.setServer_total_data(--val);
	}
		
	public void incServer_inflow(int val){		
		this.setServer_inflow((this.getServer_inflow() + val));
	}
	
	public void decServer_inflow(int val){		
		this.setServer_inflow((this.getServer_inflow() - val));
	}
	
	public void incServer_inflow(){		
		int val = this.getServer_inflow();
		this.setServer_inflow(++val);
	}
	
	public void decServer_inflow(){
		int val = this.getServer_inflow();
		this.setServer_inflow(--val);
	}
	
	public void incServer_outflow(int val){		
		this.setServer_outflow((this.getServer_outflow() + val));
	}
	
	public void decServer_outflow(int val){		
		this.setServer_outflow((this.getServer_outflow() - val));
	}
	
	public void incServer_outflow(){		
		int val = this.getServer_outflow();
		this.setServer_outflow(++val);
	}
	
	public void decServer_outflow(){
		int val = this.getServer_outflow();
		this.setServer_outflow(--val);
	}
	
	public void show(Cluster c) {
		for(int p_id : this.getServer_partitions()) {				
			Partition p = c.getPartition(p_id);				
			Global.LOGGER.info("    ----"+p.toString());
			//p.show();				
		}
	}
	
	@Override
	public String toString() {
		return (this.getServer_label()
				+"[Partition: "+this.getServer_partitions().size()
				+"| Data: "+this.getServer_total_data()
				+"| Load: "+this.getServer_load()+"%"
				+"]"
				);
	}

	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Server)) {
			return false;
		}
		
		Server server = (Server) object;
		return (this.getServer_label().equals(server.getServer_label()));
	}

	@Override
	public int hashCode() {
		return (this.getServer_label().hashCode());
	}
	
	@Override
	public int compareTo(Server server) {		
		return ((int)this.server_id < (int)server.server_id) ? -1 : 
			((int)this.server_id > (int)server.server_id) ? 1 : 0;		
	}
}