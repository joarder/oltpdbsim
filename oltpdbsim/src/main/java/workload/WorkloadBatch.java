package main.java.workload;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import umontreal.iro.lecuyer.simevents.Sim;
import main.java.cluster.Cluster;
import main.java.entry.Global;
import main.java.utils.Permutation;
import main.java.utils.VertexPair;
import main.java.utils.graph.SimGraph;
import main.java.utils.graph.SimHypergraph;
import main.java.utils.graph.SimpleEdge;
import main.java.utils.graph.SimpleGraph;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleHypergraph;
import main.java.utils.graph.SimpleVertex;

public class WorkloadBatch {
	
	private int wrl_id;
	
	private SortedMap<Integer, Map<Integer, Transaction>> trMap;
	private SortedMap<Integer, ArrayList<Integer>> trBuffer;	
	
	// Graph, Hypergraph
	public SimpleGraph<SimpleVertex, SimpleEdge> gr;
	public SimpleHypergraph<SimpleVertex, SimpleHEdge> hgr;
	public Set<Integer> hCut;
		
	// To keep track of edge id and corresponding hyperedge/transaction id
	public Map<Integer, Set<Integer>> edge_id_map;
	public Map<Integer, Integer> cEdge_id_map;
	public Map<Integer, Integer> cHEdge_id_map;
	
	private int db_tuple_counts;	
	private int wrl_total_data;	
	
	private Map<Integer, Integer> wrl_dataId_clusterId_map;	
	private Map<Integer, Integer> wrl_virtualDataId_clusterId_map;
	
	// Workload files for partitioning
	private String wrl_file_name;
	private String wrl_fixfile_name;
	private File wrl_file;
	private File wrl_fixfile;
	
	// Print Writer
	private PrintWriter wrl_prWriter;
	private PrintWriter miner_prWriter;
	
	// Performance metrics	
	private int _tr_nums;
	private int _old_dt_nums;
	private int _dt_nums;
	private int _old_ndt_nums;
	private int _ndt_nums;
	private int _old_ndti_sum;
	private double _mean_trFreq;
	private double _throughput;
	private double _response_time;
	private double _mean_dti;
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
		this.set_old_ndti_sum(0);
		
		this.setTrMap(new TreeMap<Integer, Map<Integer, Transaction>>());
		this.setTrBuffer(new TreeMap<Integer, ArrayList<Integer>>());
		
		this.setWrl_dataId_clusterId_map(new TreeMap<Integer, Integer>());
		this.setWrl_virtualDataId_clusterId_map(new TreeMap<Integer, Integer>());		
		
		switch(Global.workloadRepresentation) {
			case "gr":
				this.gr = new SimGraph<SimpleVertex, SimpleEdge>();
				this.edge_id_map = new TreeMap<Integer, Set<Integer>>();
				
				if(Global.compressionEnabled)
					this.cEdge_id_map = new TreeMap<Integer, Integer>();
					
				break;
				
			case "hgr":
				this.hgr = new SimHypergraph<SimpleVertex, SimpleHEdge>();
				
				if(Global.compressionEnabled)
					this.cHEdge_id_map = new TreeMap<Integer, Integer>();
				
				break;
		}
		
		this.hCut = new TreeSet<Integer>();
	}

	public int getWrl_id() {
		return wrl_id;
	}

	public void setWrl_id(int wrl_id) {
		this.wrl_id = wrl_id;
	}

	public SortedMap<Integer, Map<Integer, Transaction>> getTrMap() {
		return trMap;
	}

	public void setTrMap(SortedMap<Integer, Map<Integer, Transaction>> trMap) {
		this.trMap = trMap;
	}

	public SortedMap<Integer, ArrayList<Integer>> getTrBuffer() {
		return trBuffer;
	}

	public void setTrBuffer(SortedMap<Integer, ArrayList<Integer>> trBuffer) {
		this.trBuffer = trBuffer;
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

	public double get_mean_dti() {
		return _mean_dti;
	}

	public void set_mean_dti(double wrl_mean_dti) {
		this._mean_dti = wrl_mean_dti;
	}

	public double get_mean_trFreq() {
		return _mean_trFreq;
	}

	public void set_mean_trFreq(double wrl_mean_trFreq) {
		this._mean_trFreq = wrl_mean_trFreq;
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

	public int get_old_ndti_sum() {
		return _old_ndti_sum;
	}

	public void set_old_ndti_sum(int _old_ndti_sum) {
		this._old_ndti_sum = _old_ndti_sum;
	}

	public int get_ndt_nums() {
		return _ndt_nums;
	}

	public void set_ndt_nums(int wrl_ndt_nums) {
		this._ndt_nums = wrl_ndt_nums;
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

	public Map<Integer, Integer> getWrl_virtualDataId_clusterId_map() {
		return wrl_virtualDataId_clusterId_map;
	}

	public void setWrl_virtualDataId_clusterId_map(
			Map<Integer, Integer> wrl_virtualDataId_clusterId_map) {
		this.wrl_virtualDataId_clusterId_map = wrl_virtualDataId_clusterId_map;
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
		
	public void calculateAverageTrFreq(int total_transactions, int total_trFreq) {
		
		double freq = Math.round(((double) total_trFreq 
				/ (double) total_transactions) * 100.0) / 100.0;
		this.set_mean_trFreq(freq);
	}
	
	// Adding graph edges from a single transaction
	public void addGraphEdges(Transaction tr) {
		
		Set<VertexPair> permutations = Permutation.getPermutations(tr.getTr_dataSet());
		Set<Integer> edgeSet = new TreeSet<Integer>();
				
		for(VertexPair pair : permutations) {

			SimpleVertex v1 = this.gr.getVertex(pair.x);
			SimpleVertex v2 = this.gr.getVertex(pair.y);			
			SimpleEdge e = null;
			
			if(v1 != null && v2 != null)
				e = this.gr.findEdge(v1, v2);
				
			if(e != null) {
				e.incWeight(1);
				
			} else {
				
				v1 = new SimpleVertex(pair.x, 1);
				v2 = new SimpleVertex(pair.y, 1);
								
				this.gr.addEdge(new SimpleEdge(++Global.edgeSeq, tr.getTr_frequency()), v1, v2);
				edgeSet.add(Global.edgeSeq);				
			}
		}
		
		this.edge_id_map.put(tr.getTr_id(), edgeSet);
	}
	
	// Adding a hyperedge from a single transaction
	public void addHGraphEdge(Transaction tr) {	
		
		SimpleHEdge hEdge = this.hgr.getHEdge(tr.getTr_id());
				
		if(hEdge != null)
			hEdge.incWeight(1);			
		else {
			this.hgr.addHEdge(new SimpleHEdge(tr.getTr_id(), tr.getTr_frequency()), 
					this.getVertices(tr.getTr_dataSet()));
		}
	}
	
	// Converts a set of transactional data set into a set of vertices
	private Set<SimpleVertex> getVertices(Set<Integer> trDataSet) {
		
		Set<SimpleVertex> trSet = new TreeSet<SimpleVertex>();
		
		for(Integer d : trDataSet)			
			trSet.add(new SimpleVertex(d, 1)); // 1 = Vertex Weight		
		
		return trSet;		
	}
	
	public Transaction getTransaction(int tr_id) {
		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet()) {
			if(this.getTrMap().get(entry.getKey()).containsKey(tr_id))
				return this.getTrMap().get(entry.getKey()).get(tr_id); 
		}
		
		return null;
	}
	
	// Unused function
	// Remove a set of transactions from the Workload
	public void removeTransactions(Set<Integer> removed_transactions, int i) {
		HashMap<Integer, TreeSet<Integer>> _dataSetMap = new HashMap<Integer, TreeSet<Integer>>();
		
		for(int tr_id : removed_transactions) {
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
		
		for(Integer tr_id : trList) {
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
	
	// Calculate the percentage of Distributed Transactions within the Workload (before and after the Data movements)
	public void calculateDTI(Cluster cluster) {

		int dt_nums = 0;
		int dti_sum = 0;
		int ndt_nums = 0;
		int ndti_sum = 0;
		
		double dt_percentage = 0.0d;
		double ndt_percentage = 0.0d;
		double mean_dti = 0.0d;
		
		for(SimpleHEdge h : this.hgr.getEdges()) {
		
			Transaction tr = this.getTransaction(h.getId());			
			tr.calculateSpans(cluster);
			
			if(tr.isDt()) {
				++dt_nums;
				
				tr.calculateDTImapct();
				dti_sum += tr.getTr_dtImpact();
				
			} else { // Non-distributed transactions
				++ndt_nums;
				
				tr.calculateDTImapct();
				ndti_sum += tr.getTr_dtImpact();
				
			}
		} // end -- for()		
		
		this.set_dt_nums(dt_nums);
		this.set_ndt_nums(ndt_nums);
		
//		System.out.println("--> tr = "+this.hgr.getEdgeCount());
//		System.out.println("--> *tr = "+this.get_tr_nums());
//		System.out.println("--> dt = "+dt_nums+" | dti = "+dti_sum);
//		System.out.println("--> ndt = "+(ndt_nums+this.get_old_ndt_nums())
//				+" |old_ndt_nums = "+ this.get_old_ndt_nums()+" | ndt_nums = "+ndt_nums+" | ndti = "+ndti_sum);
//		
		if(this.get_dt_nums() != 0)
			dt_percentage = (Math.round(((double)dt_nums 
					/ (double)this.get_tr_nums()) * 100.00) / 100.00) * 100.00;
		System.out.println("--> % dt = "+dt_percentage);
		if(this.get_ndt_nums() != 0)
			ndt_percentage = (Math.round((((double)(ndt_nums + this.get_old_ndt_nums())) 
					/ (double)this.get_tr_nums()) * 100.0) / 100.00) * 100.00;
		System.out.println("--> % ndt = "+ndt_percentage);
		// following equation 3 of the UCC 2014 paper
		mean_dti = Math.round(((double)dti_sum 
				/ ((double)(dti_sum + ndti_sum + this.get_old_ndti_sum()))) * 100.00) / 100.00;
		
		this.set_percentage_dt(dt_percentage);
		this.set_percentage_ndt(ndt_percentage);
		this.set_mean_dti(mean_dti);
		
		//this.calculateAverageTrFreq(total_trFreq);
	}
	
	public void show() {		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet())			
			for(Entry<Integer, Transaction> e : entry.getValue().entrySet())
				Global.LOGGER.info(e.getValue().toString());
	}
}