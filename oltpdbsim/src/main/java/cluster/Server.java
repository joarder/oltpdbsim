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

package main.java.cluster;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import main.java.cluster.SSD.SSD_STATE;
import main.java.entry.Global;

public class Server implements Comparable<Server> {		
	private int server_id;
	private String server_label;
	private Map<Integer, SSD> server_SSDs;
	private Set<Integer> server_partitions;	
	private double server_load;
	private int server_inflow;
	private int server_outflow;
	private int server_total_data;
	private int server_last_assigned_ssd;
	
	public Server(int id) {
		this.setServer_id(id);
		this.setServer_label("S"+id);
		this.setServer_SSDs(new HashMap<Integer, SSD>());
		this.setServer_partitions(new HashSet<Integer>());
		this.setServer_load(0.0d);
		this.setServer_inflow(0);
		this.setServer_outflow(0);
		this.setServer_total_data(0);
		this.setServer_last_assigned_ssd(0);
		
		for(int i = 1; i <= Global.serverSSD; ++i) 
			this.getServer_SSDs().put(i, new SSD(i, id));
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

	public Map<Integer, SSD> getServer_SSDs() {
		return server_SSDs;
	}

	public void setServer_SSDs(Map<Integer, SSD> server_SSDs) {
		this.server_SSDs = server_SSDs;
	}

	public void setServer_partitions(Set<Integer> server_partitions) {
		this.server_partitions = server_partitions;
	}

	public Set<Integer> getServer_partitions() {
		return server_partitions;
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
	
	public int getServer_last_assigned_ssd() {
		return server_last_assigned_ssd;
	}

	public void setServer_last_assigned_ssd(int server_last_assigned_ssd) {
		this.server_last_assigned_ssd = server_last_assigned_ssd;
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
	
	private void updateSSDLoad(Cluster cluster, SSD ssd) {
		double load = 0.0;
		
		for(int p_id : ssd.getSSD_partitions()) {
			Partition p = cluster.getPartition(p_id);
			load += (double) p.getPartition_dataSet().size();
		}
		
		load = Math.round((load/Global.serverSSDCapacity) * 100.0) / 100.0;
		ssd.setSSD_load(load);
		
		if(ssd.getSSD_load() > 80.0)
			ssd.setSSD_state(SSD_STATE.Overloaded);
		else if(ssd.getSSD_load() >= 90.0)
			ssd.setSSD_state(SSD_STATE.Critical);
		else if(ssd.getSSD_load() >= 100.0)
			ssd.setSSD_state(SSD_STATE.Faulty);
	}
	
	public void updateServer_load(Cluster cluster) {
		
		// Updating individual SSD load
		for(Entry<Integer, SSD> ssd_entry : this.getServer_SSDs().entrySet())
			this.updateSSDLoad(cluster, ssd_entry.getValue());
		
		double load = Math.round((double) this.getServer_total_data()/(Global.serverSSD * Global.serverSSDCapacity) * 100.0) / 100.0;
		this.setServer_load(load);
	}
		
	public void show(Cluster cluster) {
		for(Entry<Integer, SSD> ssd_entry : this.getServer_SSDs().entrySet())
			Global.LOGGER.info("\t----"+ssd_entry.getValue().toString());
		
		for(int p_id : this.getServer_partitions()) {				
			Partition p = cluster.getPartition(p_id);				
			Global.LOGGER.info("\t\t----"+p.toString());
			//p.show();				
		}
	}
	
	@Override
	public String toString() {
		return (this.getServer_label()
				+" - Partitions["+this.getServer_partitions().size()+"] "
				+"| Data["+this.getServer_total_data()+"]"
				+"| Load("+this.getServer_load()+"%)");
	}

	@Override
	public boolean equals(Object object) {
		if (!(object instanceof Server))
			return false;
		
		Server server = (Server) object;
		return (this.getServer_label().equals(server.getServer_label()));
	}

	@Override
	public int hashCode() {
		return (this.getServer_label().hashCode());
	}
	
	@Override
	public int compareTo(Server server) {		
		return (this.server_id < server.server_id) ? -1 : (this.server_id > server.server_id) ? 1 : 0;		
	}
}