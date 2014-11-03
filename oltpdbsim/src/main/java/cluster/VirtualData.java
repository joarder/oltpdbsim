package main.java.cluster;

import java.util.Set;
import java.util.TreeSet;

public class VirtualData implements Comparable<VirtualData>  {
	
	private int vdata_id;	
	private int vdata_partition_id;
	private int vdata_server_id;
	private String vdata_label;	
	private Set<Integer> vdata_set;
	
	public VirtualData(int v_id, int p_id, int s_id) {		
		this.setVdata_id(v_id);
		this.setVdata_partition_id(p_id);
		this.setVdata_server_id(s_id);
		this.setVdata_set(new TreeSet<Integer>());
	}

	public int getVdata_id() {
		return vdata_id;
	}

	public void setVdata_id(int vdata_id) {
		this.vdata_id = vdata_id;
	}

	public int getVdata_partition_id() {
		return vdata_partition_id;
	}

	public void setVdata_partition_id(int vdata_partition_id) {
		this.vdata_partition_id = vdata_partition_id;
	}

	public int getVdata_server_id() {
		return vdata_server_id;
	}

	public void setVdata_server_id(int vdata_server_id) {
		this.vdata_server_id = vdata_server_id;
	}

	public Set<Integer> getVdata_set() {
		return vdata_set;
	}

	public void setVdata_set(Set<Integer> vdata_set) {
		this.vdata_set = vdata_set;
	}

	public String getVdata_label() {
		return vdata_label;
	}

	public void setVdata_label(String vdata_label) {
		this.vdata_label = vdata_label;
	}

	@Override
	public String toString() {
		String msg = "V"+this.vdata_id+" | P"+this.vdata_partition_id+" | S"+this.vdata_server_id; 
		
		return msg;
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof VirtualData)) {
			return false;
		}
		
		VirtualData vdata = (VirtualData) object;
		return this.getVdata_label().equals(vdata.getVdata_label());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		
		int result = 1;
		result = prime * result + vdata_id;
		
		return result;
	}

	@Override
	public int compareTo(VirtualData vdata) {		
		return (((int)this.getVdata_id() < (int)vdata.getVdata_id()) ? -1 : 
			((int)this.getVdata_id() > (int)vdata.getVdata_id()) ? 1 : 0);		
	}
}