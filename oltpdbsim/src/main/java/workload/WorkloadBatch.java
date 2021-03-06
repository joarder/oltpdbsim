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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.utils.graph.CompressedHEdge;
import main.java.utils.graph.CompressedVertex;
import main.java.utils.graph.ISimpleHypergraph;
import main.java.utils.graph.SimpleHypergraph;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import umontreal.iro.lecuyer.simevents.Sim;

public class WorkloadBatch {
	
	private int wrl_id;
	
	private Map<Integer, Map<Integer, Transaction>> trMap;	
	
	// Hypergraph	
	public ISimpleHypergraph<SimpleVertex, SimpleHEdge> hgr;
	
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
	private long _repartitioning_time;
	
	public WorkloadBatch(int id) {
		this.setWrl_id(id);		
		this.set_old_dt_nums(0);
		this.set_old_ndt_nums(0);		
		this.setTrMap(new HashMap<Integer, Map<Integer, Transaction>>());		
		this.setWrl_dataId_clusterId_map(new HashMap<Integer, Integer>());				
		this.hgr = new SimpleHypergraph<SimpleVertex, SimpleHEdge>();		
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

	public int get_intra_server_mgr() {
		return _intra_dmv;
	}

	public void set_intra_server_mgr(int wrl_intra_dmv) {
		this._intra_dmv = wrl_intra_dmv;
	}

	public int get_inter_server_mgr() {
		return _inter_dmv;
	}

	public void set_inter_server_mgr(int wrl_inter_dmv) {
		this._inter_dmv = wrl_inter_dmv;
	}

	public long get_repartitioning_time() {
		return _repartitioning_time;
	}

	public void set_repartitioning_time(long _repartitioning_time) {
		this._repartitioning_time = _repartitioning_time;
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
		int tr_frequency = 0;
		
		// Observed over the recent observed window
		if(Global.observationWindow > tr.getTr_period())
			tr_frequency = (int)(Global.observationWindow/tr.getTr_period());
		else
			tr_frequency = 1;
				
		if(h != null) {
			this.hgr.updateHEdgeWeight(h, tr_frequency);
			
		} else {
			this.hgr.addHEdge(new SimpleHEdge(tr.getTr_id(), tr_frequency), 
					this.getVertices(cluster, tr, tr_frequency));
		}
	}
	
	// Converts a set of transactional data set into a set of vertices
	public Set<SimpleVertex> getVertices(Cluster cluster, Transaction tr, int tr_weight) {		
		Set<SimpleVertex> trSet = new HashSet<SimpleVertex>();
		
		for(Integer d : tr.getTr_dataSet()) {
			Data data = cluster.getData(d);			
			SimpleVertex v = this.hgr.getVertex(data.getData_id());
			
			if(v == null) {
				v = new SimpleVertex(d, data.getData_compressed_data_id(), tr_weight,
					data.getData_partition_id(), data.getData_server_id());
			}
			
			trSet.add(v);			
		}
				
		return trSet;		
	}
	
	// Converts a set of transactional data set into a set of vertices
	public Set<SimpleVertex> getVertices(Cluster cluster, Map<Integer, Integer> trDataMap) {		
		Set<SimpleVertex> trSet = new HashSet<SimpleVertex>();
		
		for(Entry<Integer, Integer> d : trDataMap.entrySet()) {
			Data data = cluster.getData(d.getKey());

			trSet.add(new SimpleVertex(d.getKey(), data.getData_compressed_data_id(), d.getValue(),  // Calculated Vertex Weight
					data.getData_partition_id(), data.getData_server_id()));			
		}
		
		return trSet;		
	}
	
	// Returns a Transaction based on the given id
	public Transaction getTransaction(int tr_id) {
		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet()) {
			if(this.getTrMap().get(entry.getKey()).containsKey(tr_id))
				return this.getTrMap().get(entry.getKey()).get(tr_id); 
		}
		
		return null;
	}
	
	// Removes edges and hyperedges from the graphs and hypergraphs
	// Remove the nonDT non-movable edges from Graph and Hypergraph
	public void removeTransaction(Cluster cluster, Transaction tr) {		
		// Remove incident tr id from the data set
		removeIncidentTrId(cluster, tr);
		
		++Global.remove_count;
		this.getTrMap().get(tr.getTr_type()).remove(tr.getTr_id());
		
		SimpleHEdge h = this.hgr.getHEdge(tr.getTr_id());
		//Global.LOGGER.info(">> Removing "+h.toString()+"|"+this.hgr.getIncidentVertices(h));		
		this.hgr.removeHEdge(h);		
	}
	
	// Remove the incident transaction id
	private void removeIncidentTrId(Cluster cluster, Transaction tr) {		
		for(int d_id : tr.getTr_dataSet()) {
			Data data = cluster.getData(d_id);
			data.getData_incidentTr().remove(tr.getTr_id());
		}
	}
	
	// Add the incident transaction id
	public void addIncidentTrId(Cluster cluster, Set<Integer> trDataSet, int tr_id) {		
		for(int d : trDataSet) {
			Data data = cluster.getData(d);
			data.getData_incidentTr().add(tr_id);
		}
	}
	
	// Delete Operation
	// While deleting a Data from the Database it also has to be removed from the Workload
	public void deleteTrDataFromWorkload(int data_id) {
		this.deleteDataFromTr(data_id, getTrListForSearchedData(data_id));		
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
	private void deleteDataFromTr(int data_id, ArrayList<Integer> trList) {		
		for(int tr_id : trList) {
			//System.out.println("@ Removing "+data_id+" from T"+tr_id);
			Transaction tr = this.getTransaction(tr_id);
			tr.getTr_dataSet().remove(data_id);
		}
	}
	
	// Compressing workload hypergraph
	public void compression(boolean remove) {
		// Compressing workload hypergraph
		Global.LOGGER.info("Compressing current workload hypergrah ...");
		
		// Compressed hypergraph initialization
		this.hgr.setCHEdges(new HashMap<CompressedHEdge, Set<CompressedVertex>>());
		this.hgr.setCVertices(new HashMap<CompressedVertex, Set<CompressedHEdge>>());				
		this.hgr.setCHEMap(new HashMap<Integer, CompressedHEdge>());
		this.hgr.setCVMap(new HashMap<Integer, CompressedVertex>());		
		
		// Compressing hypergraph vertices
		/*for(SimpleVertex v : this.hgr.getVertices())
			this.hgr.addCVertex(v);*/		
				
		//Global.LOGGER.info(this.hgr.getCVertices().size()+" compressed vertices are retrieved from "+this.hgr.getVertexCount()+" hypergraph vertices.");
		
		// Compressing hyperedges
		Global.cHEdgeSeq = 0;
		for(SimpleHEdge h : this.hgr.getEdges())
			this.hgr.addCHEdge(h);
		
		Global.LOGGER.info(this.hgr.getCHEdgeMap().size()+" compressed hyperedges "
				+ "containing "+this.hgr.getCVertexMap().size()+" compressed vertices "
				+ "are created from "+this.hgr.getEdgeCount()+" hyperedges.");
		
		if(remove) {
			// Only selecting compressed hyperedges having at least two compressed vertices
			Global.LOGGER.info("Only selecting the compressed hyperedges having at least two compressed vertices ...");
			
			Set<Integer> toBeRemoved = new HashSet<Integer>();						
			for(Entry<CompressedHEdge, Set<CompressedVertex>> entry : this.hgr.getCHEdgeMap().entrySet())
				if(entry.getValue().size() < 2)
					toBeRemoved.add(entry.getKey().getId());			
			
			for(int i : toBeRemoved) {
				CompressedHEdge ch = this.hgr.getCHEMap().get(i);
				
				for(CompressedVertex cv : this.hgr.getIncidentCVertices(ch))
					this.hgr.getCVertexMap().get(cv).remove(ch);	
				
				this.hgr.getCHEMap().remove(i);
				this.hgr.getCHEdgeMap().remove(ch);
			}
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
			
			if(tr.isDt())
				++dt_nums;				
			else
				++ndt_nums;
		}	
			
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
	}
	
	public void show() {		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet())			
			for(Entry<Integer, Transaction> e : entry.getValue().entrySet())
				Global.LOGGER.info(e.getValue().toString());
	}
}