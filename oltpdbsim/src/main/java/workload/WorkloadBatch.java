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
		
	public static Map<Integer, Set<Integer>> edgeIdMap = new TreeMap<Integer, Set<Integer>>();
	public static Map<Integer, Integer> cEdgeIdMap = new TreeMap<Integer, Integer>();
	public static Map<Integer, Integer> cHEdgeIdMap = new TreeMap<Integer, Integer>();
	
	private int db_tuple_counts;	
	private int wrl_total_data;
	private int wrl_total_tr;
	private int wrl_init_total_tr;	
	private int wrl_tr_types;	
	private int wrl_tr_red;
	private int wrl_tr_green;
	private int wrl_tr_orange;
	
	private Map<Integer, Integer> wrl_dataId_clusterId_map;	
	private Map<Integer, Integer> wrl_virtualDataId_clusterId_map;	
	
	private boolean wrl_has_dmv;
	private String wrl_dmv_strategy;
	private String message = null;
	
	private String wrl_file_name;
	private String wrl_fixfile_name;
	
	private File wrl_file;
	private File wrl_fixfile;
	
	private PrintWriter wrl_prWriter;
	private PrintWriter miner_prWriter;
	
	// Performance metrics	
	private double wrl_throughput;
	private double wrl_response_time;
	private double wrl_mean_dti;
	private double wrl_mean_trFreq;
	private int wrl_dt_nums;
	private int wrl_ndt_nums;
	private double wrl_percentage_dt;
	private double wrl_percentage_ndt;
	private int wrl_intra_dmv;
	private int wrl_inter_dmv;
	private double wrl_percentage_intra_dmv;
	private double wrl_percentage_inter_dmv;
	
	public WorkloadBatch(int id) {
		this.setWrl_id(id);
		this.setTrMap(new TreeMap<Integer, Map<Integer, Transaction>>());
		this.setTrBuffer(new TreeMap<Integer, ArrayList<Integer>>());
		
		this.setWrl_dataId_clusterId_map(new TreeMap<Integer, Integer>());
		this.setWrl_virtualDataId_clusterId_map(new TreeMap<Integer, Integer>());		
		
		this.setWrl_throughput(0.0);
		this.setWrl_response_time(0.0);
		this.setWrl_mean_dti(0.0);
		this.setWrl_totalTransaction(0);
		this.setWrl_totalDataObjects(0);
		
		switch(Global.workloadRepresentation) {
			case "gr":
				this.gr = new SimGraph<SimpleVertex, SimpleEdge>();
				break;
				
			case "hgr":
				this.hgr = new SimHypergraph<SimpleVertex, SimpleHEdge>();
				break;
		}	
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

	public int getWrl_totalTransactions() {
		return wrl_total_tr;
	}

	public void setWrl_totalTransaction(int wrl_totalTransaction) {
		this.wrl_total_tr = wrl_totalTransaction;
	}
	
	public int getWrl_initTotalTransactions() {
		return wrl_init_total_tr;
	}

	public void setWrl_initTotalTransactions(int wrl_initTotalTransactions) {
		this.wrl_init_total_tr = wrl_initTotalTransactions;
	}

	public int getWrl_totalDataObjects() {
		return wrl_total_data;
	}

	public void setWrl_totalDataObjects(int wrl_totalData) {
		this.wrl_total_data = wrl_totalData;
	}
	
	public int getWrl_tr_types() {
		return wrl_tr_types;
	}

	public void setWrl_tr_types(int wrl_tr_types) {
		this.wrl_tr_types = wrl_tr_types;
	}
	
	public int getWrl_tr_red() {
		return wrl_tr_red;
	}

	public void setWrl_tr_red(int wrl_tr_red) {
		this.wrl_tr_red = wrl_tr_red;
	}

	public int getWrl_tr_green() {
		return wrl_tr_green;
	}

	public void setWrl_tr_green(int wrl_tr_green) {
		this.wrl_tr_green = wrl_tr_green;
	}

	public int getWrl_tr_orange() {
		return wrl_tr_orange;
	}

	public void setWrl_tr_orange(int wrl_tr_orange) {
		this.wrl_tr_orange = wrl_tr_orange;
	}

	public double getWrl_mean_dti() {
		return wrl_mean_dti;
	}

	public void setWrl_mean_dti(double wrl_mean_dti) {
		this.wrl_mean_dti = wrl_mean_dti;
	}

	public double getWrl_mean_trFreq() {
		return wrl_mean_trFreq;
	}

	public void setWrl_mean_trFreq(double wrl_mean_trFreq) {
		this.wrl_mean_trFreq = wrl_mean_trFreq;
	}

	public int getWrl_dt_nums() {
		return wrl_dt_nums;
	}

	public void setWrl_dt_nums(int wrl_dt_nums) {
		this.wrl_dt_nums = wrl_dt_nums;
	}
	
	public void incWrl_dt_nums() {
		int dt_nums = this.getWrl_dt_nums();
		this.setWrl_dt_nums(++dt_nums);
	}
	
	public void decWrl_dt_nums() {
		int dt_nums = this.getWrl_dt_nums();
		this.setWrl_dt_nums(--dt_nums);
	}

	public double getWrl_percentage_dt() {
		return wrl_percentage_dt;
	}

	public void setWrl_percentage_dt(double wrl_percentage_dt) {
		this.wrl_percentage_dt = wrl_percentage_dt;
	}

	public int getWrl_ndt_nums() {
		return wrl_ndt_nums;
	}

	public void setWrl_ndt_nums(int wrl_ndt_nums) {
		this.wrl_ndt_nums = wrl_ndt_nums;
	}
	
	public void incWrl_ndt_nums() {
		int ndt_nums = this.getWrl_ndt_nums();
		this.setWrl_ndt_nums(++ndt_nums);
	}
	
	public void decWrl_ndt_nums() {
		int ndt_nums = this.getWrl_ndt_nums();
		this.setWrl_ndt_nums(--ndt_nums);
	}
	
	public double getWrl_percentage_ndt() {
		return wrl_percentage_ndt;
	}

	public void setWrl_percentage_ndt(double wrl_percentage_ndt) {
		this.wrl_percentage_ndt = wrl_percentage_ndt;
	}

	public boolean isWrl_has_dmv() {
		return wrl_has_dmv;
	}

	public void setWrl_has_dmv(boolean wrl_has_dmv) {
		this.wrl_has_dmv = wrl_has_dmv;
	}

	public String getWrl_dmv_strategy() {
		return wrl_dmv_strategy;
	}

	public void setWrl_dmv_strategy(String wrl_dmv_strategy) {
		this.wrl_dmv_strategy = wrl_dmv_strategy;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}	
	
	public boolean isWrl_hasDataMoved() {
		return wrl_has_dmv;
	}

	public void setWrl_hasDataMoved(boolean wrl_hasDataMoved) {
		this.wrl_has_dmv = wrl_hasDataMoved;
	}

	public double getWrl_throughput() {
		return wrl_throughput;
	}

	public void setWrl_throughput(double wrl_throughput) {
		this.wrl_throughput = wrl_throughput;
	}

	public double getWrl_response_time() {
		return wrl_response_time;
	}

	public void setWrl_response_time(double wrl_response_time) {
		this.wrl_response_time = wrl_response_time;
	}

	public int getWrl_intra_dmv() {
		return wrl_intra_dmv;
	}

	public void setWrl_intra_dmv(int wrl_intra_dmv) {
		this.wrl_intra_dmv = wrl_intra_dmv;
	}

	public int getWrl_inter_dmv() {
		return wrl_inter_dmv;
	}

	public void setWrl_inter_dmv(int wrl_inter_dmv) {
		this.wrl_inter_dmv = wrl_inter_dmv;
	}

	public double getWrl_percentage_intra_dmv() {
		return wrl_percentage_intra_dmv;
	}

	public void setWrl_percentage_intra_dmv(double wrl_percentage_intra_dmv) {
		this.wrl_percentage_intra_dmv = wrl_percentage_intra_dmv;
	}

	public double getWrl_percentage_inter_dmv() {
		return wrl_percentage_inter_dmv;
	}

	public void setWrl_percentage_inter_dmv(double wrl_percentage_inter_dmv) {
		this.wrl_percentage_inter_dmv = wrl_percentage_inter_dmv;
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
	
	public void incWrl_totalTransaction() {
		int totalTransaction = this.getWrl_totalTransactions();		
		++totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}
	
	public void decWrl_totalTransactions() {
		int totalTransaction = this.getWrl_totalTransactions();		
		--totalTransaction;
		this.setWrl_totalTransaction(totalTransaction);
	}
	
	public void calculateAverageTrFreq(int batch_total_trFreq) {
		double freq = Math.round(((double) batch_total_trFreq / (double) this.getWrl_totalTransactions()) * 100.0) / 100.0;
		this.setWrl_mean_trFreq(freq);
	}	
	
	public void calculateThroughput(int batch_total_transactions) {
		double throughput = Math.round(((double) batch_total_transactions / (double) 3600) * 100.0) / 100.0;
		this.setWrl_throughput(throughput);
	}
	
	public void calculateResponseTime(int batch_total_transactions, double batch_total_response_time) {
		double response_time = Math.round(((double) batch_total_response_time / (double) batch_total_transactions) * 100.00) / 100.00;
		this.setWrl_response_time(response_time);
	}
	
	// Adding Graph Edges from a single Transaction
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
		
		WorkloadBatch.edgeIdMap.put(tr.getTr_id(), edgeSet);
	}
	
	// Adding a Hyperedge from a single Transaction
	public void addHGraphEdge(Transaction tr) {	
		
		SimpleHEdge hEdge = this.hgr.getHEdge(tr.getTr_id());
				
		if(hEdge != null)
			hEdge.incWeight(1);			
		else {
			this.hgr.addHEdge(new SimpleHEdge(tr.getTr_id(), tr.getTr_frequency()), 
					this.getVertices(tr.getTr_dataSet()));
		}
	}
	
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
	
	// Remove a set of transactions from the Workload
	public void removeTransactions(Set<Integer> removed_transactions, int i) {
		HashMap<Integer, TreeSet<Integer>> _dataSetMap = new HashMap<Integer, TreeSet<Integer>>();
		
		for(int tr_id : removed_transactions) {
			Transaction transaction = this.getTransaction(tr_id);
			
			Set<Integer> dataSet = new TreeSet<Integer>();
			dataSet = transaction.getTr_dataSet();
			_dataSetMap.put(tr_id, (TreeSet<Integer>) dataSet);
			
			this.getTrMap().get(i).remove(transaction.getTr_id()); // Removing Object			
			this.decWrl_totalTransactions();					
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
		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet()) {
			for(Entry<Integer, Transaction> tr_entry : entry.getValue().entrySet()) {
				
				Transaction transaction = tr_entry.getValue();
				Iterator<Integer> data =  transaction.getTr_dataSet().iterator();
				
				++Global.mining_serial;
				this.getMiner_prWriter().print(Global.mining_serial+" "+Global.mining_serial+" "+transaction.getTr_dataSet().size()+" ");
				
				while(data.hasNext()) {
					this.getMiner_prWriter().print(cluster.getData(data.next()).getData_id());
					
					if(data.hasNext())
						this.getMiner_prWriter().print(" ");
					else
						this.getMiner_prWriter().println();
				}
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
		int total_trFreq = 0;
		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet()) {			
			for(Entry<Integer, Transaction> e : entry.getValue().entrySet()) {
				
				Transaction tr = e.getValue();				
				tr.calculateSpans(cluster);				
				total_trFreq += tr.getTr_frequency();
				
				if(tr.getTr_serverSpanCost() > 1) {
					++dt_nums;
					
					tr.calculateDTImapct();
					dti_sum += tr.getTr_dtImpact();
					
				} else { // Non-distributed transactions
					++ndt_nums;
					
					tr.calculateDTImapct();
					ndti_sum += tr.getTr_dtImpact();
					
				}
			} // end -- for()-Transaction		
		} // end -- for()-
		
		this.setWrl_dt_nums(dt_nums);
		this.setWrl_ndt_nums(ndt_nums);
		
		if(this.getWrl_dt_nums() != 0)
			dt_percentage = (Math.round(((double)dt_nums / (double)this.getWrl_totalTransactions()) * 100.00) / 100.00) * 100.00;
		
		if(this.getWrl_ndt_nums() != 0)
			ndt_percentage = (Math.round(((double)ndt_nums / (double)this.getWrl_totalTransactions()) * 100.0) / 100.00) * 100.00;
				
		this.setWrl_percentage_dt(dt_percentage);
		this.setWrl_percentage_ndt(ndt_percentage);
		
		mean_dti = Math.round(((double)dti_sum / ((double)(dti_sum + ndti_sum))) * 100.00) / 100.00;	// following equation 3 of the UCC 2014 paper			
		this.setWrl_mean_dti(mean_dti);
		
		this.calculateAverageTrFreq(total_trFreq);
	}
	
	public void show() {		
		for(Entry<Integer, Map<Integer, Transaction>> entry : this.getTrMap().entrySet())			
			for(Entry<Integer, Transaction> e : entry.getValue().entrySet())
				Global.LOGGER.info(e.getValue().toString());
	}
}