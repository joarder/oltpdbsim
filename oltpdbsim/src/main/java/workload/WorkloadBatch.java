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

package main.java.workload;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.utils.graph.ISimpleHypergraph;
import main.java.utils.graph.SimHypergraph;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import umontreal.iro.lecuyer.simevents.Sim;

public class WorkloadBatch {
	
	private int wrl_id;
	
	private Map<Integer, Map<Integer, Transaction>> trMap;	
	
	// Hypergraph	
	public ISimpleHypergraph<SimpleVertex, SimpleHEdge> hgr;
		
	// To keep track of edge id and corresponding hyperedge/transaction id
	public Map<Integer, Set<Integer>> edge_id_map;
	public Map<Integer, Integer> cEdge_id_map;
	public Map<Integer, Integer> cHEdge_id_map;
	
	private int db_tuple_counts;	
	private int wrl_total_data;	
	
	private Map<Integer, Integer> wrl_dataId_clusterId_map;
	
	// Workload files for partitioning
	private String wrl_file_name;
	private String wrl_fixfile_name;
	private File wrl_file;
	private File wrl_fixfile;
	
	// Print Writer
	private PrintWriter wrl_prWriter;
	private PrintWriter miner_prWriter;
	
	// Performance metrics
	private double idt;
	private int _tr_nums;
	private int _old_dt_nums;
	private int _dt_nums;
	private int _old_ndt_nums;
	private int _ndt_nums;
	private int _dt_original;
	private int _dt_new;
	private int _edges_in_cut;
	private double _throughput;
	private double _response_time;
	private double _percentage_dt;
	private double _percentage_ndt;
	private int _intra_dmv;
	private int _inter_dmv;
	private double _percentage_intra_dmv;
	private double _percentage_inter_dmv;
	
	public WorkloadBatch(int id) {
		this.setWrl_id(id);
		
		this.set_old_dt_nums(0);
		this.set_old_ndt_nums(0);
		
		this.setTrMap(new HashMap<Integer, Map<Integer, Transaction>>());
		
		this.setWrl_dataId_clusterId_map(new HashMap<Integer, Integer>());		
		
		this.hgr = new SimHypergraph<SimpleVertex, SimpleHEdge>();		
		
		switch(Global.workloadRepresentation) {
			case "gr":
				if(Global.compressionEnabled)
					this.cEdge_id_map = new HashMap<Integer, Integer>();
					
				break;
				
			case "hgr":
				if(Global.compressionEnabled)
					this.cHEdge_id_map = new HashMap<Integer, Integer>();
				
				break;
		}
	}

	public int getWrl_id() {
		return wrl_id;
	}

	public void setWrl_id(int wrl_id) {
		this.wrl_id = wrl_id;
	}

	public Map<Integer, Map<Integer, Transaction>> getTrMap() {
		return trMap;
	}

	public void setTrMap(Map<Integer, Map<Integer, Transaction>> trMap) {
		this.trMap = trMap;
	}

	public int getDb_tuple_counts() {
		return db_tuple_counts;
	}

	public void setDb_tuple_counts(int db_tuple_counts) {
		this.db_tuple_counts = db_tuple_counts;
	}

	public int getWrl_totalDataObjects() {
		return wrl_total_data;
	}

	public void setWrl_totalDataObjects(int wrl_totalData) {
		this.wrl_total_data = wrl_totalData;
	}

	public int get_tr_nums() {
		return _tr_nums;
	}

	public void set_tr_nums(int wrl_tr_nums) {
		this._tr_nums = wrl_tr_nums;
	}

	public int get_old_dt_nums() {
		return _old_dt_nums;
	}

	public void set_old_dt_nums(int _old_dt_nums) {
		this._old_dt_nums = _old_dt_nums;
	}

	public int get_dt_nums() {
		return _dt_nums;
	}

	public void set_dt_nums(int wrl_dt_nums) {
		this._dt_nums = wrl_dt_nums;
	}
	
	public int get_dt_new() {
		return _dt_new;
	}

	public void set_dt_new(int _dt_new) {
		this._dt_new = _dt_new;
	}

	public void inc_dt_nums() {
		int dt_nums = this.get_dt_nums();
		this.set_dt_nums(++dt_nums);
	}
	
	public void dec_dt_nums() {
		int dt_nums = this.get_dt_nums();
		this.set_dt_nums(--dt_nums);
	}

	public double get_percentage_dt() {
		return _percentage_dt;
	}

	public void set_percentage_dt(double wrl_percentage_dt) {
		this._percentage_dt = wrl_percentage_dt;
	}

	public int get_old_ndt_nums() {
		return _old_ndt_nums;
	}

	public void set_old_ndt_nums(int _old_ndt_nums) {
		this._old_ndt_nums = _old_ndt_nums;
	}

	public int get_dt_original() {
		return _dt_original;
	}

	public void set_dt_original(int _dt_original) {
		this._dt_original = _dt_original;
	}

	public int get_ndt_nums() {
		return _ndt_nums;
	}

	public void set_ndt_nums(int wrl_ndt_nums) {
		this._ndt_nums = wrl_ndt_nums;
	}
	
	public int get_edges_in_cut() {
		return _edges_in_cut;
	}

	public void set_edges_in_cut(int _edges_in_cut) {
		this._edges_in_cut = _edges_in_cut;
	}

	public void inc_ndt_nums() {
		int ndt_nums = this.get_ndt_nums();
		this.set_ndt_nums(++ndt_nums);
	}
	
	public void dec_ndt_nums() {
		int ndt_nums = this.get_ndt_nums();
		this.set_ndt_nums(--ndt_nums);
	}
	
	public double get_percentage_ndt() {
		return _percentage_ndt;
	}

	public void set_percentage_ndt(double wrl_percentage_ndt) {
		this._percentage_ndt = wrl_percentage_ndt;
	}

	public double getIdt() {
		return idt;
	}

	public void setIdt(double idt) {
		this.idt = idt;
	}

	public double get_throughput() {
		return _throughput;
	}

	public void set_throughput(double wrl_throughput) {
		this._throughput = wrl_throughput;
	}

	public double get_response_time() {
		return _response_time;
	}

	public void set_response_time(double wrl_response_time) {
		this._response_time = wrl_response_time;
	}

	public int get_intra_dmv() {
		return _intra_dmv;
	}

	public void set_intra_dmv(int wrl_intra_dmv) {
		this._intra_dmv = wrl_intra_dmv;
	}

	public int get_inter_dmv() {
		return _inter_dmv;
	}

	public void set_inter_dmv(int wrl_inter_dmv) {
		this._inter_dmv = wrl_inter_dmv;
	}

	public double get_percentage_intra_dmv() {
		return _percentage_intra_dmv;
	}

	public void set_percentage_intra_dmv(double wrl_percentage_intra_dmv) {
		this._percentage_intra_dmv = wrl_percentage_intra_dmv;
	}

	public double get_percentage_inter_dmv() {
		return _percentage_inter_dmv;
	}

	public void set_percentage_inter_dmv(double wrl_percentage_inter_dmv) {
		this._percentage_inter_dmv = wrl_percentage_inter_dmv;
	}

	public Map<Integer, Integer> getWrl_dataId_clusterId_map() {
		return wrl_dataId_clusterId_map;
	}

	public void setWrl_dataId_clusterId_map(
			Map<Integer, Integer> wrl_dataId_clusterId_map) {
		this.wrl_dataId_clusterId_map = wrl_dataId_clusterId_map;
	}

	public String getWrl_file_name() {
		return wrl_file_name;
	}

	public void setWrl_file_name(String wrl_file_name) {
		this.wrl_file_name = wrl_file_name;
	}

	public String getWrl_fixfile_name() {
		return wrl_fixfile_name;
	}

	public void setWrl_fixfile_name(String wrl_fixfile_name) {
		this.wrl_fixfile_name = wrl_fixfile_name;
	}

	public File getWrl_file() {
		return wrl_file;
	}

	public void setWrl_file(File wrl_file) {
		this.wrl_file = wrl_file;
	}

	public File getWrl_fixfile() {
		return wrl_fixfile;
	}

	public void setWrl_fixfile(File wrl_fixfile) {
		this.wrl_fixfile = wrl_fixfile;
	}

	public PrintWriter getWrl_prWriter() {
		return wrl_prWriter;
	}

	public void setWrl_prWriter(PrintWriter wrl_prWriter) {
		this.wrl_prWriter = wrl_prWriter;
	}

	public PrintWriter getMiner_prWriter() {
		return miner_prWriter;
	}

	public void setMiner_prWriter(PrintWriter miner_prWriter) {
		this.miner_prWriter = miner_prWriter;
	}	
	
	public void calculateThroughput(int total_transactions) {
		
		double throughput = Math.round(((double) total_transactions 
				/ (double) Sim.time()) * 100.0) / 100.0;
		this.set_throughput(throughput);
	}
	
	public void calculateResponseTime(int total_transactions, double total_response_time) {
		
		double response_time = Math.round(((double) total_response_time 
				/ (double) total_transactions) * 100.00) / 100.00;
		this.set_response_time(response_time);
	}
	
	// Adding a hyperedge from a single transaction
	public void addHGraphEdge(Cluster cluster, Transaction tr) {	
		
		SimpleHEdge h = this.hgr.getHEdge(tr.getTr_id());
		int tr_frequency = (int)(Global.observationWindow/tr.getTr_period());
				
		if(h != null) {
			this.hgr.updateHEdgeWeight(h, tr_frequency);
			
		} else {
			this.hgr.addHEdge(new SimpleHEdge(tr.getTr_id(), tr_frequency), 
					this.getVertices(cluster, tr.getTr_dataSet()));
		}
	}
	
	// Converts a set of transactional data set into a set of vertices
	public Set<SimpleVertex> getVertices(Cluster cluster, Set<Integer> trDataSet) {		
		Set<SimpleVertex> trSet = new HashSet<SimpleVertex>();
		
		for(Integer d : trDataSet) {
			Data data = cluster.getData(d);			
			SimpleVertex v = this.hgr.getVertex(data.getData_id());
			
			if(v == null)
				v = new SimpleVertex(d, 1,  // 1 : Initial Vertex weight
					data.getData_partition_id(), data.getData_server_id());
			
			trSet.add(v);			
		}
				
		return trSet;		
	}
	
	// Converts a set of transactional data set into a set of vertices
	public Set<SimpleVertex> getVertices(Cluster cluster, Map<Integer, Integer> trDataMap) {		
		Set<SimpleVertex> trSet = new TreeSet<SimpleVertex>();
		
		for(Entry<Integer, Integer> d : trDataMap.entrySet()) {
			Data data = cluster.getData(d.getKey());
			
			trSet.add(new SimpleVertex(d.getKey(), d.getValue(),  // Calculated Vertex Weight
					data.getData_partition_id(), data.getData_server_id()));
		}
		
		return trSet;		
	}
	
	public Transaction getTransaction(int tr_id) {
		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet()) {
			if(this.getTrMap().get(entry.getKey()).containsKey(tr_id))
				return this.getTrMap().get(entry.getKey()).get(tr_id); 
		}
		
		return null;
	}
	
	private void removeIncidentTrId(Cluster cluster, Transaction tr) {
		for(int d_id : tr.getTr_dataSet()) {
			Data data = cluster.getData(d_id);
			data.getData_incidentTr().remove(tr.getTr_id());
		}
	}
	
	// Removes edges and hyperedges from the graphs and hypergraphs
	// Remove the nonDT non-movable edges from Graph and Hypergraph
	public void removeTransaction(Cluster cluster, Transaction tr) {
				
		// Remove incident tr id from the data set
		removeIncidentTrId(cluster, tr);
		
		++Global.remove_count;
		this.getTrMap().get(tr.getTr_type()).remove(tr.getTr_id());
		
		SimpleHEdge h = this.hgr.getHEdge(tr.getTr_id());
		//System.out.println(">> Removing "+h.toString()+"|"+this.hgr.getIncidentVertices(h));
		this.hgr.removeHEdge(h);		
	}	
	
	// Unused function
	// Remove a set of transactions from the Workload
	public void removeTransactions(Set<Integer> removed_transactions, int i) {
		HashMap<Integer, TreeSet<Integer>> _dataSetMap = new HashMap<Integer, TreeSet<Integer>>();
		
		for(int tr_id : removed_transactions) {
			++Global.remove_count;
			Transaction transaction = this.getTransaction(tr_id);
			
			Set<Integer> dataSet = new TreeSet<Integer>();
			dataSet = transaction.getTr_dataSet();
			_dataSetMap.put(tr_id, (TreeSet<Integer>) dataSet);
			
			this.getTrMap().get(i).remove(transaction.getTr_id()); // Removing Object			
			//this.decWrl_totalTransactions();					
		}
	}	
	
	// Returns a list of transaction ids which contain the searched data id (Only at the time of new Transaction generation)
	public ArrayList<Integer> getTrListForSearchedData(int data_id) {
		
		ArrayList<Integer> trList = new ArrayList<Integer>();		
		
		for(Entry<Integer, Map<Integer, Transaction>> e : this.getTrMap().entrySet())
			for(Entry<Integer, Transaction> tr : e.getValue().entrySet())
				if(tr.getValue().getTr_dataSet().contains(data_id))
					trList.add(tr.getValue().getTr_id());
		
		return trList;
	}
	
	// Remove a data id from the given list of transactions (Only at the time of new Transaction generation)
	public void deleteDataFromTr(int data_id, ArrayList<Integer> trList) {
		
		for(int tr_id : trList) {
			//System.out.println("@ Removing "+data_id+" from T"+tr_id);
			Transaction tr = this.getTransaction(tr_id);
			tr.getTr_dataSet().remove(data_id);
		}
	}
	
	public void deleteTrDataFromWorkload(int data_id) {
		this.deleteDataFromTr(data_id, getTrListForSearchedData(data_id));		
	}
	
	// Write workloads into file for stream mining
	public void prepareMiningFile(Cluster cluster) {		
				
		for(SimpleHEdge h : this.hgr.getEdges()) {
			
			Transaction tr = this.getTransaction(h.getId());				
			Iterator<Integer> data =  tr.getTr_dataSet().iterator();
			
			++Global.mining_serial;
			this.getMiner_prWriter().print(Global.mining_serial+" "+Global.mining_serial+" "
											+tr.getTr_dataSet().size()+" ");
			
			while(data.hasNext()) {
				this.getMiner_prWriter().print(cluster.getData(data.next()).getData_id());
				
				if(data.hasNext())
					this.getMiner_prWriter().print(" ");
				else
					this.getMiner_prWriter().println();
			}
		}		
		
		this.getMiner_prWriter().flush();
		this.getMiner_prWriter().close();
	}
	
	// Add the incident transaction id
	public void addIncidentTr(Cluster cluster, Set<Integer> trDataSet, int trId) {
		
		for(int d : trDataSet) {		
			Data data = cluster.getData(d);
			data.getData_incidentTr().add(trId);
		}
	}
	
	// Calculate the percentage of Distributed Transactions within the Workload (before and after the Data movements)
	public void calculateDTI(Cluster cluster) {

		int dt_nums = 0;
		int ndt_nums = 0;
		
		double dt_percentage = 0.0d;
		double ndt_percentage = 0.0d;
		
		for(SimpleHEdge h : this.hgr.getEdges()) {
			
			Transaction tr = this.getTransaction(h.getId());
			tr.calculateSpans(cluster);
			
			if(tr.isDt()) {
				++dt_nums;
				
				//if(Global.compressionBeforeSetup)
					//this.sword.hCut.add(h);
				
			} else { // Non-distributed transactions
				++ndt_nums;
			}
		} // end -- for()		
			
		this.set_tr_nums(this.hgr.getEdgeCount());
		this.set_edges_in_cut(dt_nums);
		this.set_dt_nums(dt_nums);
		this.set_ndt_nums(ndt_nums);
		
		if(this.get_dt_nums() != 0)
			dt_percentage = ((double)(dt_nums + this.get_old_dt_nums()) 
					/ (double)(this.get_tr_nums() + this.get_old_dt_nums() + this.get_old_ndt_nums())) * 100.00;

		if(this.get_ndt_nums() != 0)
			ndt_percentage = ((double)(ndt_nums + this.get_old_ndt_nums()) 
					/ (double)(this.get_tr_nums() + this.get_old_dt_nums() + this.get_old_ndt_nums())) * 100.0;
		
		this.set_percentage_dt(dt_percentage);
		this.set_percentage_ndt(ndt_percentage);
		
		//if(Global.compressionBeforeSetup)
			//this.sword.calculateContribution(cluster, this);
	}
	
	public void show() {		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet())			
			for(Entry<Integer, Transaction> e : entry.getValue().entrySet())
				Global.LOGGER.info(e.getValue().toString());
	}
}