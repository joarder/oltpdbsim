package main.java.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class Partition implements Comparable<Partition> {
	private int partition_id;
	private String partition_label;
	
	private int partition_server_id;	
	
	private long partition_start_key;
	private long partition_end_key;
	
	private Map<Integer, Data> partition_data_set;
	private Map<Integer, Integer> partition_data_lookup_table;
	
	private int partition_home_data;
	private int partition_foreign_data;
	private int partition_roaming_data;
	
	private int partition_inflow;
	private int partition_outflow;
	
	public Partition(int p_id) {
		this.setPartition_id(p_id);
		this.setPartition_label("P"+p_id);
		
		this.setPartition_serverId(0);		
		
		this.setPartition_dataSet(new HashMap<Integer, Data>());
		this.setPartition_dataLookupTable(new HashMap<Integer, Integer>());
		
		this.setPartition_home_data(0);
		this.setPartition_foreign_data(0);
		this.setPartition_roaming_data(0);
		
		this.setPartition_inflow(0);
		this.setPartition_outflow(0);		
	}

	public int getPartition_id() {
		return partition_id;
	}

	public void setPartition_id(int partition_id) {
		this.partition_id = partition_id;
	}

	public String getPartition_label() {
		return partition_label;
	}

	public void setPartition_label(String partition_label) {
		this.partition_label = partition_label;
	}

	public int getPartition_serverId() {
		return partition_server_id;
	}

	public void setPartition_serverId(int partition_server_id) {
		this.partition_server_id = partition_server_id;
	}

	public long getPartition_start_key() {
		return partition_start_key;
	}

	public void setPartition_start_key(long partition_start_key) {
		this.partition_start_key = partition_start_key;
	}

	public long getPartition_end_key() {
		return partition_end_key;
	}

	public void setPartition_end_key(long partition_end_key) {
		this.partition_end_key = partition_end_key;
	}

	public Map<Integer, Data> getPartition_dataSet() {
		return this.partition_data_set;
	}

	public void setPartition_dataSet(Map<Integer, Data> partition_data_set) {
		this.partition_data_set = partition_data_set;
	}
	
	public Map<Integer, Integer> getPartition_dataLookupTable() {
		return partition_data_lookup_table;
	}

	public void setPartition_dataLookupTable(Map<Integer, Integer> data_lookup_table) {
		this.partition_data_lookup_table = data_lookup_table;
	}
	
	public int getPartition_home_data() {
		return partition_home_data;
	}

	public void setPartition_home_data(int partition_home_data) {
		this.partition_home_data = partition_home_data;
	}

	public int getPartition_foreign_data() {
		return partition_foreign_data;
	}

	public void setPartition_foreign_data(int partition_foreign_data) {
		this.partition_foreign_data = partition_foreign_data;
	}

	public int getPartition_roaming_data() {
		return partition_roaming_data;
	}

	public void setPartition_roaming_data(int partition_roaming_data) {
		this.partition_roaming_data = partition_roaming_data;
	}
	
	public int getPartition_inflow() {
		return partition_inflow;
	}

	public void setPartition_inflow(int partition_inflow) {
		this.partition_inflow = partition_inflow;
	}

	public int getPartition_outflow() {
		return partition_outflow;
	}

	public void setPartition_outflow(int partition_outflow) {
		this.partition_outflow = partition_outflow;
	}
	
	public void incPartition_inflow(int val){		
		this.setPartition_inflow((this.getPartition_inflow() + val));
	}
	
	public void decPartition_inflow(int val){		
		this.setPartition_inflow((this.getPartition_inflow() - val));
	}
	
	public void incPartition_inflow(){		
		int val = this.getPartition_inflow();
		this.setPartition_inflow(++val);
	}
	
	public void decPartition_inflow(){
		int val = this.getPartition_inflow();
		this.setPartition_inflow(--val);
	}

	public void incPartition_outflow(int val){		
		this.setPartition_outflow((this.getPartition_outflow() + val));
	}
	
	public void decPartition_outflow(int val){		
		this.setPartition_outflow((this.getPartition_outflow() - val));
	}
	
	public void incPartition_outflow(){		
		int val = this.getPartition_outflow();
		this.setPartition_outflow(++val);
	}
	
	public void decPartition_outflow(){
		int val = this.getPartition_outflow();
		this.setPartition_outflow(--val);
	}
	
	public void incPartition_home_data() {
		int home_data = this.getPartition_home_data();		
		this.setPartition_home_data(++home_data);
	}
	
	public void decPartition_home_data() {
		int home_data = this.getPartition_home_data();		
		this.setPartition_home_data(--home_data);
	}
	
	public void incPartition_foreign_data() {
		int foreign_data = this.getPartition_foreign_data();		
		this.setPartition_foreign_data(++foreign_data);
	}
	
	public void decPartition_foreign_data() {
		int foreign_data = this.getPartition_foreign_data();		
		this.setPartition_foreign_data(--foreign_data);
	}
	
	public void incPartition_roaming_data() {
		int roaming_data = this.getPartition_roaming_data();		
		this.setPartition_roaming_data(++roaming_data);
	}
	
	public void decPartition_roaming_data() {
		int roaming_data = this.getPartition_roaming_data();		
		this.setPartition_roaming_data(--roaming_data);
	}	
	
	// Returns a Data object queried by it's Data id from the Partition
	public Data getData(Cluster cluster, int data_id) {
		Data d =null;
		int roaming_partition_id = -1;
		Partition roaming_partition = null;
		
		// new
		if(this.getPartition_dataSet().containsKey(data_id)) {
			d = this.getPartition_dataSet().get(data_id);
			
		} else {			
			// If Data is not found in its Home Partition then search in the Roaming Table entry and 
			// corresponding Roaming Partition		
			roaming_partition_id = this.getPartition_dataLookupTable().get(data_id);
			roaming_partition = cluster.getPartition(roaming_partition_id);
			
			return roaming_partition.getData(cluster, data_id);
		}
		
		return d;
	}
		
	public void show() {
		int comma = this.getPartition_dataSet().size();
		
		System.out.print("       {");
		
		//for(Data data : this.getPartition_dataSet()) {
		for(Entry<Integer, Data> e : this.getPartition_dataSet().entrySet()) {
			System.out.print(e.getValue().toString());
			
			if(comma != 1)
				System.out.print(", ");
			
			--comma;
		}
		
		System.out.println("}");
	}

	@Override
	public String toString() {
		if(this.getPartition_roaming_data() != 0 || this.getPartition_foreign_data() !=0)
			return (this.getPartition_label()
					+"[Data: "+this.getPartition_dataSet().size()+"] - "
					+"[Lookup Table: "+this.getPartition_dataLookupTable().size()+"] - "
					+"[R: "+this.getPartition_roaming_data()+"]"
					+"[F: "+this.getPartition_foreign_data()+"] - " 
					+"[Inflow: "+this.getPartition_inflow()+"]"
					+"[Outflow: "+this.getPartition_outflow()+"]"
					);										
		else	
			return (this.getPartition_label()
					+"[Data: "+this.getPartition_dataSet().size()+"] - "
					+"[Lookup Table: "+this.getPartition_dataLookupTable().size()+"]");
	}
	
	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Partition)) {
			return false;
		}
		
		Partition partition = (Partition) object;
		return (this.getPartition_label().equals(partition.getPartition_label()));
	}

	@Override
	public int hashCode() {
		return (this.getPartition_label().hashCode());
	}

	@Override
	public int compareTo(Partition partition) {		
		return (((int)this.getPartition_id() < (int)partition.getPartition_id()) ? -1 : 
			((int)this.getPartition_id() > (int)partition.getPartition_id()) ? 1 : 0);		
	}
}