package main.java.cluster;

import java.util.HashSet;
import java.util.Set;

public class CompressedData implements Comparable<CompressedData>  {
	
	private int cData_id;
	private String cData_uid;
	private int cData_partition_id;
	private int cData_server_id;
	private String cData_label;	
	private Set<Integer> cData_set;
	
	public CompressedData(int c_id, String c_uid) {		
		this.setCData_id(c_id);
		this.setCData_uid(c_uid);
		this.setCData_partition_id(-1);
		this.setCData_server_id(-1);
		this.setCData_set(new HashSet<Integer>());
	}

	public int getCData_id() {
		return cData_id;
	}

	public void setCData_id(int cData_id) {
		this.cData_id = cData_id;
	}

	public String getCData_uid() {
		return cData_uid;
	}

	public void setCData_uid(String cData_uid) {
		this.cData_uid = cData_uid;
	}

	public int getVdata_partition_id() {
		return cData_partition_id;
	}

	public void setCData_partition_id(int cData_partition_id) {
		this.cData_partition_id = cData_partition_id;
	}

	public int getVdata_server_id() {
		return cData_server_id;
	}

	public void setCData_server_id(int cData_server_id) {
		this.cData_server_id = cData_server_id;
	}

	public Set<Integer> getCData_set() {
		return cData_set;
	}

	public void setCData_set(Set<Integer> cData_set) {
		this.cData_set = cData_set;
	}

	public String getVdata_label() {
		return cData_label;
	}

	public void setVdata_label(String cData_label) {
		this.cData_label = cData_label;
	}

	@Override
	public String toString() {
		return ("CD"+this.cData_id+" | P"+this.cData_partition_id+" | S"+this.cData_server_id); 
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof CompressedData)) {
			return false;
		}
		
		CompressedData cData = (CompressedData) object;
		return this.getVdata_label().equals(cData.getVdata_label());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		
		int result = 1;
		result = prime * result + cData_id;
		
		return result;
	}

	@Override
	public int compareTo(CompressedData cData) {		
		return ((this.getCData_id() < cData.getCData_id()) ? -1 : 
			(this.getCData_id() > cData.getCData_id()) ? 1 : 0);		
	}
}