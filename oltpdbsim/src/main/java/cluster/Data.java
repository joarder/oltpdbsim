package main.java.cluster;

public class Data implements Comparable<Data> {
	private int data_id;
	private int data_weight;
	
	private int data_table_id;
	private int data_vdata_id; // used for SWORD only
	private int data_partion_id;
	private int data_server_id;
	private String data_label;
	
	// HyperGraph and Graph Partitioning Attributes
	private int data_hmetis_cluster_id;
	private int data_chmetis_cluster_id;
	private int data_metis_cluster_id;
	private int data_shadow_id;
	private int data_virtual_node_id;
	private boolean data_hasShadowId;
	private boolean data_inUse;
	// 	
	private int data_home_partition_id;				// Original Home Partition Id
	private int data_home_server_id;				// Original Home Server Id
	// Roaming Attributes
	private boolean data_isRoaming;
	private boolean data_isMoveable;	
	
	public Data(int d_id, int t_id, int v_id, int p_id, int s_id) {
		this.setData_id(d_id);
		this.setData_weight(1);
		
		this.setData_table_id(t_id);
		this.setData_vdata_id(v_id);
		this.setData_partion_id(p_id);
		this.setData_server_id(s_id);
		this.setData_label("d"+this.getData_id());
		
		this.setData_hmetisClusterId(-1);
		this.setData_chmetisClusterId(-1);
		this.setData_metisClusterId(-1);
		this.setData_shadowId(-1);		
		this.setData_virtual_data_id(-1);
		this.setData_hasShadowId(false);
		this.setData_inUse(false);
		
		this.setData_home_partition_id(p_id);
		this.setData_home_server_id(s_id);
			
		this.setData_isRoaming(false);
		this.setData_isMoveable(true);
	}

	public String getData_label() {
		return data_label;
	}

	public void setData_label(String data_label) {
		this.data_label = data_label;
	}

	public int getData_id() {
		return data_id;
	}

	public void setData_id(int data_id) {
		this.data_id = data_id;
	}

	public int getData_weight() {
		return data_weight;
	}

	public void setData_weight(int data_weight) {
		this.data_weight = data_weight;
	}

	public int getData_table_id() {
		return data_table_id;
	}

	public void setData_table_id(int data_table_id) {
		this.data_table_id = data_table_id;
	}

	public int getData_vdata_id() {
		return data_vdata_id;
	}

	public void setData_vdata_id(int data_vdata_id) {
		this.data_vdata_id = data_vdata_id;
	}

	public int getData_partition_id() {
		return data_partion_id;
	}

	public void setData_partion_id(int data_partion_id) {
		this.data_partion_id = data_partion_id;
	}

	public int getData_server_id() {
		return data_server_id;
	}

	public void setData_server_id(int data_server_id) {
		this.data_server_id = data_server_id;
	}
	
	public int getData_hmetisClusterId() {
		return data_hmetis_cluster_id;
	}

	public void setData_hmetisClusterId(int data_hmetis_cluster_id) {
		this.data_hmetis_cluster_id = data_hmetis_cluster_id;
	}

	public int getData_chmetisClusterId() {
		return data_chmetis_cluster_id;
	}

	public void setData_chmetisClusterId(int data_chmetis_cluster_id) {
		this.data_chmetis_cluster_id = data_chmetis_cluster_id;
	}

	public int getData_metisClusterId() {
		return data_metis_cluster_id;
	}

	public void setData_metisClusterId(int data_metis_cluster_id) {
		this.data_metis_cluster_id = data_metis_cluster_id;
	}

	public int getData_shadowId() {
		return data_shadow_id;
	}

	public void setData_shadowId(int data_shadow_id) {
		this.data_shadow_id = data_shadow_id;
	}

	public int getData_virtual_data_id() {
		return data_virtual_node_id;
	}

	public void setData_virtual_data_id(int data_virtual_node_id) {
		this.data_virtual_node_id = data_virtual_node_id;
	}

	public boolean isData_hasShadowId() {
		return data_hasShadowId;
	}

	public void setData_hasShadowId(boolean data_hasShadowId) {
		this.data_hasShadowId = data_hasShadowId;
	}
	
	public boolean isData_inUse() {
		return data_inUse;
	}

	public void setData_inUse(boolean data_inUse) {
		this.data_inUse = data_inUse;
	}

	public int getData_homePartitionId() {
		return data_home_partition_id;
	}

	public void setData_home_partition_id(int data_home_partition_id) {
		this.data_home_partition_id = data_home_partition_id;
	}

	public int getData_homeNodeId() {
		return data_home_server_id;
	}

	public void setData_home_server_id(int data_home_node_id) {
		this.data_home_server_id = data_home_node_id;
	}
	
	public boolean isData_isRoaming() {
		return data_isRoaming;
	}

	public void setData_isRoaming(boolean data_isRoaming) {
		this.data_isRoaming = data_isRoaming;
	}
	
	public boolean isData_isMoveable() {
		return data_isMoveable;
	}

	public void setData_isMoveable(boolean data_isMoveable) {
		this.data_isMoveable = data_isMoveable;
	}

	@Override
	public String toString() {		
		return (this.data_label);
	}
		
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Data)) {
			return false;
		}
		
		Data data = (Data) object;
		return this.getData_label().equals(data.getData_label());
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + data_id;
		return result;
	}

	@Override
	public int compareTo(Data data) {		
		return (((int)this.getData_id() < (int)data.getData_id()) ? -1 : 
			((int)this.getData_id() > (int)data.getData_id()) ? 1:0);		
	}
}