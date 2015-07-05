package main.java.cluster;

import java.util.HashSet;
import java.util.Set;

public class SSD implements Comparable<SSD> {
	enum SSD_STATE {Healthy, Overloaded, Critical, Faulty};
	
	private String ssd_label;
	private int ssd_id;
	private int ssd_server_id;
	private Set<Integer> ssd_partitions;	
	private double ssd_load;	
	private SSD_STATE ssd_state;	
	
	public SSD(int id, int server_id) {
		this.setSSD_label("SSD"+id+"-S"+server_id);
		this.setSSD_id(id);
		this.setSSD_server_id(server_id);
		this.setSSD_load(0.0);
		this.setSSD_partitions(new HashSet<Integer>());
		this.setSSD_state(SSD_STATE.Healthy);
	}		

	public String getSSD_label() {
		return ssd_label;
	}

	public void setSSD_label(String ssd_label) {
		this.ssd_label = ssd_label;
	}

	public int getSSD_id() {
		return ssd_id;
	}

	public void setSSD_id(int ssd_id) {
		this.ssd_id = ssd_id;
	}
	
	public int getSSD_server_id() {
		return ssd_server_id;
	}

	public void setSSD_server_id(int ssd_server_id) {
		this.ssd_server_id = ssd_server_id;
	}

	public Set<Integer> getSSD_partitions() {
		return ssd_partitions;
	}

	public void setSSD_partitions(Set<Integer> ssd_partitions) {
		this.ssd_partitions = ssd_partitions;
	}

	public double getSSD_load() {
		return ssd_load;
	}

	public void setSSD_load(double ssd_load) {
		this.ssd_load = ssd_load;
	}

	public SSD_STATE getSSD_state() {
		return ssd_state;
	}

	public void setSSD_state(SSD_STATE ssd_state) {
		this.ssd_state = ssd_state;
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof SSD))
			return false;
		
		SSD ssd = (SSD) object;
		return this.getSSD_label().equals(ssd.getSSD_label());
	}	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + this.getSSD_id() + this.getSSD_server_id();
		return result;
	}

	@Override
	public int compareTo(SSD ssd) {
		int id1 = this.getSSD_id() + this.getSSD_server_id();
		int id2 = ssd.getSSD_id() + ssd.getSSD_server_id();
		
		return ((id1 < id2) ? -1 : (id1 > id2) ? 1 : 0);
	}

	@Override
	public String toString() {
		return (this.getSSD_label()
				+":Partitions["+this.getSSD_partitions()+"]/"
					+ "Load("+this.getSSD_load()+"%)-"
						+ "STATE["+this.getSSD_state()+"]");
	}
}