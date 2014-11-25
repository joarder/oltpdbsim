package main.java.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import main.java.db.Database;
import main.java.db.Table;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.repartition.DataMovement;
import main.java.repartition.MinCut;
import main.java.repartition.WorkloadBatchProcessor;
import main.java.utils.Utility;
import main.java.workload.Transaction;
import main.java.workload.Workload;
import main.java.workload.WorkloadBatch;

public class Cluster {	
	private SortedSet<Server> servers;
	private SortedSet<Partition> partitions;
	private Map<Integer, VirtualData> vdataSet;
	
	private ConsistentHashRing<Long> cluster_ring;
	private Map<Long, Integer> ring_map;
	private Map<Integer, ArrayList<Integer>> partition_map;
	private Map<Integer, ArrayList<Long>> partition_keyRange;

	public static boolean _setup;
	
	public Cluster() {
	    this.setPartitions(new TreeSet<Partition>());
		this.setServers(new TreeSet<Server>());		
		this.setRing(new ConsistentHashRing<Long>(Global.replicas));
		this.setRing_map(new HashMap<Long, Integer>());
		this.setPartition_map(new HashMap<Integer, ArrayList<Integer>>());
		this.setPartition_keyRange(new HashMap<Integer, ArrayList<Long>>());
		
		if(Global.compressionBeforeSetup)
			this.setVdataSet(new TreeMap<Integer, VirtualData>());		
	}
	
	public SortedSet<Server> getServers() {
		return servers;
	}

	public void setServers(SortedSet<Server> servers) {
		this.servers = servers;
	}

	public SortedSet<Partition> getPartitions() {
		return partitions;
	}

	public void setPartitions(SortedSet<Partition> partitions) {
		this.partitions = partitions;
	}

	public Map<Integer, VirtualData> getVdataSet() {
		return vdataSet;
	}

	public void setVdataSet(Map<Integer, VirtualData> vdataSet) {
		this.vdataSet = vdataSet;
	}

	public ConsistentHashRing<Long> getRing() {
		return cluster_ring;
	}

	public void setRing(ConsistentHashRing<Long> ring) {
		this.cluster_ring = ring;
	}

	public Map<Long, Integer> getRing_map() {
		return ring_map;
	}

	public void setRing_map(Map<Long, Integer> ring_map) {
		this.ring_map = ring_map;
	}

	public Map<Integer, ArrayList<Integer>> getPartition_map() {
		return partition_map;
	}

	public void setPartition_map(Map<Integer, ArrayList<Integer>> partition_map) {
		this.partition_map = partition_map;
	}

	public Map<Integer, ArrayList<Long>> getPartition_keyRange() {
		return partition_keyRange;
	}

	public void setPartition_keyRange(Map<Integer, ArrayList<Long>> partition_keyRange) {
		this.partition_keyRange = partition_keyRange;
	}

	public void setup(Database db, Workload wrl) {
		// Will only be used for SWORD		
		Global.LOGGER.info("Setting up Cluster ...");
		
		// Add Partitions
		Global.LOGGER.info("-----------------------------------------------------------------------------");
	    Global.LOGGER.info("Creating "+Global.partitions+" fixed number of logical Partitions ...");
	    
		for(int i = 1; i <= Global.partitions; i++)
			this.getPartitions().add(new Partition(i));
		
		// Add Servers and fixed amount of Partitions
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Adding "+Global.servers+" physical Servers in the Cluster ...");
		
		for(int i = 1; i <= Global.servers; i++)
			this.addServer(new Server(i), true);			
			
		// Assign Partitions into Servers
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Assigning logical Partitions into physical Servers ...");
		
		long p_min = 0;
		//long p_max = Global.max_keyvalue; // Long.MAX_VALUE;
		long p_max = Long.MAX_VALUE;
		long p_size = (p_max - p_min) / Global.partitions;
		ArrayList<Long> p_keyRange = null;
		
		Global.partition_capacity = p_size;
			
		// Define Partition range
		for(Partition p: this.getPartitions()) {
			// Assign a Server id to the Partition
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Assigning Partition "+p.getPartition_label()+" in a Server ...");
			this.assignPartition(p);				
			
			// Calculate Key Range values for individual Partition
			Global.LOGGER.info(".............................................................................");
			Global.LOGGER.info("Defining Key range for "+p.getPartition_label()+" ...");
			p_keyRange = new ArrayList<Long>();
			
			p.setPartition_start_key(p_min);
			p_keyRange.add(p_min);
			
			p_min += p_size;
			p.setPartition_end_key(p_min);
			p_keyRange.add(p_min);
			
			// Added in the Consistent Ring
			this.getRing().add(p_min);
			this.getRing_map().put(p_min, p.getPartition_id());
			
			Global.LOGGER.info(p.getPartition_label()+" key: "+p_min);
			
			p_min += 1;
			
			this.getPartition_keyRange().put(p.getPartition_id(), p_keyRange);
			
			Global.LOGGER.info("Key range for "+p.getPartition_label()+": "
					+"Start["+p.getPartition_start_key()+"], "
					+"End["+p.getPartition_end_key()+"]");			
		}
		
		//
		if(Global.compressionEnabled)
			Global.virtualNodes = ((int) db.getDb_tuple_counts() / (int) Global.compressionRatio);
				
		// Physical Data Distribution
		if(Global.compressionBeforeSetup)
			this.vdataDistribution(db, this, wrl);
		else {
			this.dataDistribution(db);
			//this.warmup(db, wrl);
		}
		
		// Update server-level load statistic and show
		this.updateLoad();
		this.show();
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total data within the Cluster: "+Global.global_dataCount);
		Global.LOGGER.info("Cluster setup has finished.");
	}

	// Physical Data distribution
	private void dataDistribution(Database db) {

		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Starting physical Data distribution within the Partitions ...");

		// Create and assign Data into Partitions		
		for(Entry<Integer, Table> tbl_entry : db.getDb_tables().entrySet()) {			
			Table tbl = tbl_entry.getValue();
			
			for(Entry<Integer, Tuple> tpl : tbl.getTbl_tuples().entrySet()) {				

				if(!tpl.getValue().getTuple_action().equals("insert"))					
					this.insertData(tpl.getValue().getTuple_id());
			}
						
			Global.LOGGER.info("Data distribution has completed for Table '"+tbl.getTbl_name()+"'.");
			Global.LOGGER.info(""+tbl.toString());
		}
	}
	
	// Physical Data distribution for SWORD (Compression before setup)
	private void vdataDistribution(Database db, Cluster cluster, Workload wrl) {
		_setup = true;
		
		//Global.virtualNodes = ((int) db.getDb_tuple_counts() / (int) Global.compressionRatio);
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");		
		Global.LOGGER.info("Creating "+Global.virtualNodes+" virtual nodes from "
				+db.getDb_tuple_counts()+" tuples ...");
		
		// Initial Data distribution
		this.dataDistribution(db);

		// Stream a new Workload Batch
		Global.global_trSeq = 0;
		WorkloadBatch wb = this.warmupSword(db, this, wrl);
		
		Global.LOGGER.info("Total "+Global.global_trSeq+" transactions containing "
				+wb.getWrl_totalDataObjects()+" data rows have generated.");
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		
		// Single compressed hypergraph partitioning using k-way min-cut		
		// Generate workload file
		boolean empty = false;
		try {
			empty = WorkloadBatchProcessor.generateWorkloadFile(cluster, wb);
			
		} catch (IOException e) {			
			Global.LOGGER.error("Error in creating workload file !!!", e);
		}
		
		if(!empty) {
			int partitions = Global.partitions;
			Global.LOGGER.info("Repartitioning the workload into "+partitions+" clusters ...");	
			
			// Perform hyper-graph/graph/compressed hyper-graph partitioning			 
			MinCut.runMinCut(wb, partitions, true);

			// Mapping cluster id to partition id
			Global.LOGGER.info("Applying data movement strategies for database ("+db.getDb_name()+") ...");
			
			try {
				WorkloadBatchProcessor.processPartFile(cluster, wb, partitions);
			} catch (IOException e) {
				Global.LOGGER.error("Error in processing part file !!!", e);
			}					
			
			// Perform data movement			
			DataMovement.performDataMovement(cluster, wb, "random", Global.workloadRepresentation);					
			
			// Update server-level load statistic and show
			cluster.updateLoad();
			//cluster.show();
			
			Global.LOGGER.info("=======================================================================================================================");
		}
		
		// Initialize Sword's incremental repartitioning algorithm		
		wb.sword.init(cluster, wb);
		
		_setup = false;
	}
	
	private WorkloadBatch warmupSword(Database db, Cluster cluster, Workload wrl) {		
		WorkloadBatch wb = new WorkloadBatch(0);
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Initiating SWORD based virtual node distribution ...");
		Global.LOGGER.info("Targeting 1hr workload generation ...");
		
		// i -- Transaction types
		for(int i = 1; i <= wrl.tr_types; i++) {
			// Calculate the number of transactions to be created for a specific type
			int tr_nums = (int) Math.ceil(wrl.tr_proportions.get(i) * 3600); // 3600 transactions ~ 1hr workload			
			Global.LOGGER.info("Generating "+tr_nums+" transactions of type "+i+" ...");

			// j -- number of Transactions for a specific Transaction type
			for(int j = 1; j <= tr_nums; j++) {		
				Set<Integer> trTupleSet = wrl.getTrTupleSet(db, i);
				Set<Integer> trDataSet = Workload.getTrDataSet(db, cluster, wb, trTupleSet);
				
				if(!wb.getTrMap().containsKey(i))
					wb.getTrMap().put(i, new TreeMap<Integer, Transaction>());	
				
				++Global.global_trSeq;
				Transaction tr = new Transaction(Global.global_trSeq, i, trDataSet, -1);
				
				wb.getTrMap().get(i).put(tr.getTr_id(), tr);
				
				// Add a hyperedge to Workload Hypergraph
				wb.addHGraphEdge(tr);
			}			
		}

		wb.setWrl_totalDataObjects(Global.global_dataCount);
		wb.setDb_tuple_counts(db.getDb_tuple_counts());
		
		return wb;
	}
	
	public void warmup(Database db, Workload wrl) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Warming up the database ...");
		Global.LOGGER.info("Targeting "+1000+" transaction generation ...");		

		wrl.warmingup = true;
		
		// i -- Transaction types
		for(int i = 1; i <= wrl.tr_types; i++) {
			// Calculate the number of transactions to be created for a specific type
			int tr_nums = (int) Math.ceil(wrl.tr_proportions.get(i) * 3600); // 3600 transactions ~ 1hr workload			
			Global.LOGGER.info("Generating "+tr_nums+" transactions of type "+i+" ...");

			// j -- number of Transactions for a specific Transaction type
			for(int j = 1; j <= tr_nums; j++) {		
				wrl.getTrTupleSet(db, i);
			}			
		}
		
		wrl.warmingup = false;
		db.updateTupleCounts();
	}		
	
	//
	public int getDataIdFromTupleId(int tpl_id) {
		String[] parts = this.breakDataId(tpl_id);
		int d_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		// Always returns the first replica
		return Integer.parseInt((Integer.toString(d_pk)+Integer.toString(1)+Integer.toString(tbl_id)));
	}
	
	// Insert a new Data and its replicas in the Cluster
	public int insertData(int _id) {
		++Global.global_dataCount;
		
		int first_replica = 0;		
		int p_id = 0;
		
		long p_key = 0;
		long hash;
		
		String d_id = null;
		String v_id = null;		
		
		Data d = null;
		VirtualData v = null;
		
		Partition p = null;
		Server s = null;

		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = this.breakDataId(_id);
		int d_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		// Create a new Data object and its replicas
		for(int repl = 1; repl <= Global.replicas; repl++) {
			d_id = Integer.toString(d_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
			
			if(Global.compressionBeforeSetup) {				
				v_id = Integer.toString(
						Utility.simpleHash(Integer.parseInt(d_id), Global.virtualNodes));
				hash = Utility.sha1Hash(v_id);
				
			} else
				hash = Utility.sha1Hash(d_id);
			
			// Find the corresponding Partition id
			//hash = Utility.md5Hash(d_id);
			//hash = Utility.sha1Hash(d_id);
			p_key = this.getRing().get(hash);
			p_id = this.getRing_map().get(p_key);
			p = this.getPartition(p_id);
			
			// New Data
			if(Global.compressionBeforeSetup) {
				d = new Data(Integer.parseInt(d_id), tbl_id, Integer.parseInt(v_id), 
						p.getPartition_id(), p.getPartition_serverId());
				
				// Create a Virtual Data if required
				if(!this.vdataSet.containsKey(Integer.parseInt(v_id)))					
					v = new VirtualData(Integer.parseInt(v_id), p.getPartition_id(), 
							p.getPartition_serverId());
				else
					v = this.vdataSet.get(Integer.parseInt(v_id));
				
				// Put an entry in the Virtual Data's list
				v.getVdata_set().add(d.getData_id());
								
				// Assign Virtual Data to Cluster
				this.getVdataSet().put(Integer.parseInt(v_id), v);
				
				// Assign Data to Partition
				p.getPartition_dataSet().put(d.getData_id(), d);
				
			} else {
				d = new Data(Integer.parseInt(d_id), tbl_id, -1, p.getPartition_id(), 
						p.getPartition_serverId());
				
				// Assign Data to Partition
				p.getPartition_dataSet().put(d.getData_id(), d);
			}
			
			// Update Server statistic
			s = this.getServer(p.getPartition_serverId());
			s.incServer_totalData();
			
			if(repl == 1)
				first_replica = d.getData_id();
		}
		
		return first_replica;
	}
	
	// Delete a Data and all of its replicas from the Cluster
	public void deleteData(int _id) {
		--Global.global_dataCount;
		
		int v_id = 0;		
		int p_id = 0;		
		
		long p_key = 0;
		long hash;
		
		String d_id = null;
		Data d = null;
		Partition p = null;
		Server s = null;

		// Break up the Tuple id to extract the actual Tuple id, Table id and Replica id
		String[] parts = this.breakDataId(_id);
		int d_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		for(int repl = 1; repl <= Global.replicas; repl++) {
			d_id = Integer.toString(d_pk)+Integer.toString(repl)+Integer.toString(tbl_id);			
			
			if(Global.compressionBeforeSetup) {
				v_id = Utility.simpleHash(Integer.parseInt(d_id), Global.virtualNodes);
				hash = Utility.sha1Hash(Integer.toString(v_id));
				
			} else {				
				//hash = Utility.md5Hash(d_id);
				hash = Utility.sha1Hash(d_id);
			}
			
			// Find the corresponding Partition id
			p_key = this.getRing().get(hash);
			p_id = this.getRing_map().get(p_key);
			p = this.getPartition(p_id);
			
			// Delete Data from Partition
			d = p.getData(this, Integer.parseInt(d_id));
			p.getPartition_dataSet().remove(d.getData_id());
			
			if(p.getPartition_dataLookupTable().containsKey(d.getData_id()))
				p.getPartition_dataLookupTable().remove(d.getData_id());
			
			// Update Server statistic
			s = this.getServer(p.getPartition_serverId());
			s.decServer_totalData();
		}
	}
	
	// Extract actual Tuple id, Table id and Replica id from a given Data id
	public String[] breakDataId(int data_id) {
		String[] parts = new String[2];
		
		// Extract the last part from tuple id
		String d_id = Long.toString(data_id);
		int length = d_id.length();
		
		// Extract primary key and table id from the tuple id string
		parts[0] = StringUtils.substring(d_id, 0, (length - 1));
		parts[1] = StringUtils.substring(d_id, (length - 1), length);		
		
		return parts;
	}
	
	// Returns a Data object by its id
	public Data getData(int data_id) {
		
		int v_id = 0;
		int p_id = 0;		
		long p_key = 0;
		long hash;				
		Partition p = null;
		
		if(Global.compressionBeforeSetup) {						
			v_id = Utility.simpleHash(data_id, Global.virtualNodes);			
			hash = Utility.sha1Hash(Integer.toString(v_id));
			
		} else {
			//hash = Utility.md5Hash(Integer.toString(data_id));
			hash = Utility.sha1Hash(Integer.toString(data_id));
		}
	
		// Find the corresponding Partition id
		p_key = this.getRing().get(hash);
		p_id = this.getRing_map().get(p_key);
		p = this.getPartition(p_id);
		
		return p.getData(this, data_id);		
	}
	
	// Returns a Partition by its id
	public Partition getPartition(int id) {
		for(Partition p : this.getPartitions())
			if(p.getPartition_id() == id)
				return p;
		
		return null;
	}
	
	@SuppressWarnings("unused")
	private int inPartition(long x) {
		for(Entry<Integer, ArrayList<Long>> e : this.getPartition_keyRange().entrySet())
			if(Utility.inRange(e.getValue().get(0), e.getValue().get(1), x))
				return e.getKey();
		
		return -1;
	}
	
	// Update statistic for the Servers
	public void updateLoad() {
		for(Server s : this.getServers())
			s.updateServer_load();
		
		//for(Partition p : this.getPartitions())
			//p.updatePartition_load();
	}
	
	// Currently only supports initial add
	public void addServer(Server s, boolean init) {		
		this.getServers().add(s);
		
		if(!init) {
			++Global.servers;
		
			// Re-shuffling all the Partitions
			Global.LOGGER.info("Reshuffling all the Partitions in the Cluster ...");
			this.shufflePartitions();
		}
		
		Global.LOGGER.info("Server "+s.getServer_label()+" is added in the Cluster.");
	}
	
	private void shufflePartitions() {
		for(Partition p : this.getPartitions())
			this.assignPartition(p);
	}
	
	private void assignPartition(Partition p) {
		int s_id = 0;
		
		// Assign Server id to the Partition
		s_id = (p.getPartition_id() % Global.servers) + 1;
		p.setPartition_serverId(s_id);
		
		// Add Partition id in the corresponding Server's list 
		Server s = this.getServer(s_id);
		s.getServer_partitions().add(p.getPartition_id());		
		
		// Add an entry in the Cluster's Server-Partition Mapping Table
		this.addPartitionMappingEntry(s_id, p.getPartition_id());
		
		Global.LOGGER.info("Partition "+p.getPartition_label()
				+" is assigned to Server "+s.getServer_label()+".");
	}
	
	// Add an entry in the Cluster's Server-Partition Mapping Table
	private void addPartitionMappingEntry(int s_id, int p_id) {		
		if(this.getPartition_map().containsKey(s_id))
			this.getPartition_map().get(s_id).add(p_id);
		else {
			ArrayList<Integer> p_list = new ArrayList<Integer>();
			p_list.add(p_id);				
			this.getPartition_map().put(s_id, p_list);
		}
		
		Global.LOGGER.info("Server-Partition mapping has updated.");
	}
	
	public void removeServer(Server server) {
		this.getServers().remove(server);
	}
	
	public Server getServer(int id) {
		for(Server server : this.getServers())
			if(server.getServer_id() == id)
				return server;
		
		return null;
	}
	
	public void show() {
		Global.LOGGER.info("<-- Cluster Status -->");
		Global.LOGGER.info("Number of Servers: "+this.getServers().size());
		Global.LOGGER.info("Number of Partitions: "+this.getPartitions().size());
		Global.LOGGER.info("Total data: "+Global.global_dataCount);
		
		// Server Details
		for(Server s : this.getServers()) {						
			Global.LOGGER.info("    --"+s.toString());
			s.show(this);
		}		
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
	}
}