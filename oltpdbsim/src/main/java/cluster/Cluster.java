package main.java.cluster;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import main.java.db.Database;
import main.java.db.Table;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.repartition.DataMigration;
import main.java.repartition.MinCut;
import main.java.repartition.WorkloadBatchProcessor;
import main.java.utils.Utility;
import main.java.workload.Transaction;
import main.java.workload.Workload;
import main.java.workload.WorkloadBatch;

import org.apache.commons.lang3.StringUtils;

public class Cluster {	
	private SortedSet<Server> servers;
	private SortedSet<Partition> partitions;
	
	private Map<Integer, ArrayList<Integer>> partition_map;
	private Map<Integer, HashSet<Integer>> data_map;
	
	private Map<Integer, VirtualData> vdataSet;
	
	private ConsistentHashRing<Long> cluster_ring;
	private Map<Long, Integer> ring_map;
	private Map<Integer, ArrayList<Long>> partition_keyRange;
	
	private Map<Integer, String> data_uid_map;
	private Map<Integer, Integer> vdata_pid_map;
	private Map<Integer, String> vdata_uid_map;

	public static boolean _setup;
	
	public Cluster() {
		
		switch(Global.setup) {
			case "range":
				setData_map(new HashMap<Integer, HashSet<Integer>>());
				break;
			
			case "consistenthash":
				this.setRing(new ConsistentHashRing<Long>(Global.replicas));
				this.setRing_map(new HashMap<Long, Integer>());				
				this.setPartition_keyRange(new HashMap<Integer, ArrayList<Long>>());
				this.setData_uid_map(new HashMap<Integer, String>());
				break;
				
			default:
				Global.LOGGER.error("Wrong cluster setup method is specified !!! Choose either 'range' or 'consistenthash'");
				break;
		}
		
	    this.setPartitions(new TreeSet<Partition>());
		this.setServers(new TreeSet<Server>());		
		this.setPartition_map(new HashMap<Integer, ArrayList<Integer>>());
		
		if(Global.compressionBeforeSetup) {
			this.setVdataSet(new HashMap<Integer, VirtualData>());
			this.setVdata_pid_map(new HashMap<Integer, Integer>());
			this.setVdata_uid_map(new HashMap<Integer, String>()); // For consistent hashing
		}
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

	public Map<Integer, HashSet<Integer>> getData_map() {
		return data_map;
	}

	public void setData_map(Map<Integer, HashSet<Integer>> data_map) {
		this.data_map = data_map;
	}

	public Map<Integer, ArrayList<Long>> getPartition_keyRange() {
		return partition_keyRange;
	}

	public void setPartition_keyRange(Map<Integer, ArrayList<Long>> partition_keyRange) {
		this.partition_keyRange = partition_keyRange;
	}

	public Map<Integer, String> getData_uid_map() {
		return data_uid_map;
	}

	public void setData_uid_map(Map<Integer, String> data_uid_map) {
		this.data_uid_map = data_uid_map;
	}

	public Map<Integer, Integer> getVdata_pid_map() {
		return vdata_pid_map;
	}

	public void setVdata_pid_map(Map<Integer, Integer> vdata_pid_map) {
		this.vdata_pid_map = vdata_pid_map;
	}

	public Map<Integer, String> getVdata_uid_map() {
		return vdata_uid_map;
	}

	public void setVdata_uid_map(Map<Integer, String> vdata_uid_map) {
		this.vdata_uid_map = vdata_uid_map;
	}

//====================================================================================================
	public WorkloadBatch setup(Database db, Workload wrl) {
		WorkloadBatch wb = null;
		
		switch(Global.setup) {
		case "range":
			wb = this.setupRange(db, wrl);
			break;
		
		case "consistenthash":
			wb = this.setupConsistentHash(db, wrl);
			break;
			
		default:
			Global.LOGGER.error("Wrong cluster setup method is specified !!! Choose either 'range' or 'consistenthash'");
			break;
		}
		
		return wb;
	}

//====================================================================================================	
	// Range partitioning based setup
	private WorkloadBatch setupRange(Database db, Workload wrl) {
		// Will only be used for SWORD
		WorkloadBatch wb = null;
		
		Global.LOGGER.info("Setting up Cluster ...");
		
		// Add Partitions
		Global.LOGGER.info("-----------------------------------------------------------------------------");
	    Global.LOGGER.info("Creating "+Global.partitions+" fixed number of logical Partitions ...");
	    
		for(int i = 1; i <= db.getDb_tables().size()*Global.servers; i++)
			this.getPartitions().add(new Partition(i));
		
		// Add Servers and fixed amount of Partitions
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Adding "+Global.servers+" physical Servers in the Cluster ...");
		
		for(int i = 1; i <= Global.servers; i++)
			this.addServer(new Server(i), true);			
			
		// Assign Partitions into Servers
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Assigning logical Partitions into physical Servers ...");
		
		int s_id = 1;
		int tbl_id = 1;
		
		for(Partition p : this.getPartitions()) {
			// Assign a Server id to the Partition
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Assigning Partition "+p.getPartition_label()+" in a Server ...");
			
			p.setPartition_serverId(s_id);
			p.setPartition_table_id(tbl_id);
			
			// Add Partition id in the corresponding Server's list 
			Server s = this.getServer(s_id);
			s.getServer_partitions().add(p.getPartition_id());		
			
			// Add an entry in the Cluster's Server-Partition Mapping Table
			this.addPartitionMappingEntry(s_id, p.getPartition_id());
			
			Table tbl = db.getTable(tbl_id);
			Global.LOGGER.info("Partition "+p.getPartition_label()+" of "+tbl.toString()+" is assigned to Server "+s.getServer_label()+".");
			
			++s_id;			
			if(s_id > this.getServers().size())
				s_id = 1;
			
			++tbl_id;
			if(tbl_id > db.getDb_tables().size())
				tbl_id = 1;
		}
				
		// Determine the number of Virtual Data Nodes to be created
		if(Global.compressionEnabled)
			Global.virtualDataNodes = ((int) db.getDb_tuple_counts() / (int) Global.compressionRatio);
				
		// Physical Data Distribution
		this.physicalDataDistribution(db);
		
		// Virtual Data Distribution for Sword
		if(Global.compressionBeforeSetup)
			wb = this.vdataDistribution(db, this, wrl);		
		
		// Update server-level load statistic and show
		this.updateLoad();
		this.show();
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total data within the Cluster: "+Global.global_dataCount);
		Global.LOGGER.info("Cluster setup has finished.");
		
		return wb;
	}

//====================================================================================================
	// Consistent hashing based setup
	private WorkloadBatch setupConsistentHash(Database db, Workload wrl) {
		// Will only be used for SWORD
		WorkloadBatch wb = null;
		
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
		
		long p_uid = -1;
		long p_min = Long.MIN_VALUE;
		long p_max = Long.MAX_VALUE; //Long.MAX_VALUE; //1048576L; // 2^20 //1073741824; // 2^30
		long p_size = (long)((p_max/Global.partitions)+1)*2;
		
		ArrayList<Long> p_keyRange = null;		
		Global.partition_capacity = p_size;
		Global.LOGGER.info("Define a partition size of "+p_size);	
		
		// Define Partition range
		for(Partition p : this.getPartitions()) {
			// Assign a Server id to the Partition
			Global.LOGGER.info("-----------------------------------------------------------------------------");
			Global.LOGGER.info("Assigning Partition "+p.getPartition_label()+" in a Server ...");
			this.assignPartitionConsistentHash(p);				
			
			// Calculate Key Range values for individual Partition
			Global.LOGGER.info(".............................................................................");
			Global.LOGGER.info("Defining Key range for "+p.getPartition_label()+" ...");
			p_keyRange = new ArrayList<Long>();
			
			p.setPartition_start_key(p_min);
			p_keyRange.add(p_min);
			
			p_min += p_size;
			p_uid = p_min - 1;
			
			p.setPartition_end_key(p_uid);
			p.set_uid(p_uid);
			p_keyRange.add(p_uid);

			// Added in the Consistent Ring
			this.getRing().add(p_uid);
			this.getRing_map().put(p_uid, p.getPartition_id());
			
			this.getPartition_keyRange().put(p.getPartition_id(), p_keyRange);
			
			Global.LOGGER.info("Key range for "+p.getPartition_label()+": "
					+"Start["+p.getPartition_start_key()+"], "
					+"End["+p.getPartition_end_key()+"]");			
		}
		
		// Determine the number of Virtual Data Nodes to be created
		if(Global.compressionEnabled)
			Global.virtualDataNodes = ((int) db.getDb_tuple_counts() / (int) Global.compressionRatio);
				
		// Physical Data Distribution
		this.physicalDataDistribution(db);
		
		// Virtual Data Distribution for Sword
		if(Global.compressionBeforeSetup)
			wb = this.vdataDistribution(db, this, wrl);		
		
		// Update server-level load statistic and show
		this.updateLoad();
		this.show();
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Total data within the Cluster: "+Global.global_dataCount);
		Global.LOGGER.info("Cluster setup has finished.");
		
		return wb;
	}

//====================================================================================================
	public int getRangePartition(Server s, int tbl_id) {
		for(Integer p_id : s.getServer_partitions()) {
			
			Partition p = this.getPartition(p_id);
			
			if(p.getPartition_table_id() == tbl_id)
				return p.getPartition_id();
		}
		
		return 0;
	}
	
	// Physical Data distribution
	private void physicalDataDistribution(Database db) {

		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Starting physical Data distribution within the Partitions ...");

		// Create and assign Data into Partitions
		int s_id = 0;
		for(Entry<Integer, Table> tbl_entry : db.getDb_tables().entrySet()) {			
			Table tbl = tbl_entry.getValue();
			
			for(Entry<Integer, Tuple> tpl : tbl.getTbl_tuples().entrySet()) {				

				if(!tpl.getValue().getTuple_action().equals("insert")) {
					
					switch(Global.setup) {
						case "range":
							int tbl_id = tbl.getTbl_id();
							
							++s_id;
							if(s_id > Global.servers)
								s_id = 1;
							
							Server s = this.getServer(s_id);
							int p_id = this.getRangePartition(s, tbl_id);			
							
							this.insertDataRangePartitioning(tpl.getValue().getTuple_id(), s_id, p_id);
							
							break;
						
						case "consistenthash":
							this.insertDataConsistentHash(tpl.getValue().getTuple_id());
							break;
							
						default:
							Global.LOGGER.error("Wrong cluster setup method is specified !!! Choose either 'range' or 'consistenthash'");
							break;
					}					
				}
			}
						
			Global.LOGGER.info("Data distribution has completed for Table '"+tbl.getTbl_name()+"'.");
			Global.LOGGER.info(""+tbl.toString());
		}
	}

//====================================================================================================
	// Physical Data distribution for SWORD (Compression before setup)
	private WorkloadBatch vdataDistribution(Database db, Cluster cluster, Workload wrl) {
		_setup = true;
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");		
		Global.LOGGER.info("Creating "+Global.virtualDataNodes+" compressed vertices from "
									  +db.getDb_tuple_counts()+" tuples ...");
		
		// Initial Data distribution
		//this.physicalDataDistribution(db);

		// Stream a new Workload Batch
		Global.global_trSeq = 0;
		WorkloadBatch wb = this.warmupSword(db, this, wrl);
		
		Global.LOGGER.info("Total "+Global.global_trSeq+" transactions containing "
								   +wb.getWrl_totalDataObjects()+" data rows have streamed and processed.");
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		
		// A single compressed hypergraph partitioning using k-way min-cut		
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
			DataMigration.performDataMigration(cluster, wb);					
			
			// Update server-level load statistic and show
			cluster.updateLoad();
			cluster.show();
			
			Global.LOGGER.info("=======================================================================================================================");
		}
		
		// Initialize Sword's incremental repartitioning algorithm		
		//wb.sword.init(cluster, wb);
		
		_setup = false;
		
		return wb;
	}

//====================================================================================================
	private WorkloadBatch warmupSword(Database db, Cluster cluster, Workload wrl) {	
		Global.sword_cluster_setup = true;
		WorkloadBatch wb = new WorkloadBatch(0);
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Initiating SWORD based virtual node distribution ...");
		Global.LOGGER.info("Targeting 1hr workload streaming ...");
		
		// i -- Transaction types
		for(int i = 1; i <= wrl.tr_types; i++) {
			// Calculate the number of transactions to be created for a specific type
			int tr_nums = (int) Math.ceil(wrl.tr_proportions.get(i) * Global.observationWindow/4); // 3600 transactions ~ 1hr workload			
			Global.LOGGER.info("Streaming "+tr_nums+" transactions of type "+i+" ...");

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
				wb.addHGraphEdge(this, tr);
			}			
		}

		wb.setWrl_totalDataObjects(Global.global_dataCount);
		wb.setDb_tuple_counts(db.getDb_tuple_counts());
		
		Global.sword_cluster_setup = false;
		return wb;
	}
	
//====================================================================================================	
	public void warmup(Database db, Workload wrl) {
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Warming up the workload ...");
		Global.LOGGER.info("Targeting "+1000+" transaction generation ...");		

		wrl.warmingup = true;
		
		// i -- Transaction types
		for(int i = 1; i <= wrl.tr_types; i++) {
			// Calculate the number of transactions to be created for a specific type
			int tr_nums = (int) Math.ceil(wrl.tr_proportions.get(i) * Global.observationWindow); // 3600 transactions ~ 1hr workload			
			Global.LOGGER.info("Streaming "+tr_nums+" transactions of type "+i+" ...");

			// j -- number of Transactions for a specific Transaction type
			for(int j = 1; j <= tr_nums; j++) {		
				wrl.getTrTupleSet(db, i);
			}			
		}
		
		wrl.warmingup = false;
		db.updateTupleCounts();
	}
	
//====================================================================================================
	public int insertDataRangePartitioning(int _id, int s_id, int p_id) {
		++Global.global_dataCount;
		
		int[] replicas = new int[Global.replicas];
		
		Data d = null;
		VirtualData v = null;

		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = this.breakDataId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		Server s = this.getServer(s_id);
		Partition p = this.getPartition(p_id);
		
		// Create a new Data object and its replicas
		for(int repl = 1; repl <= Global.replicas; repl++) {
			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
						
			if(Global.compressionBeforeSetup) {
				
				int v_id = Utility.simpleHash(tpl_pk, Global.virtualDataNodes);
				
				// Create a Virtual Data if required
				if(!this.vdataSet.containsKey(v_id)) {					
					v = new VirtualData(v_id, null);
					this.vdataSet.put(v_id, v);
					
				} else {
					v = this.vdataSet.get(v_id);
				}
			
				// Create an entry in the vData---Partition map
				this.getVdata_pid_map().put(v_id, p_id);
				
				// Create the new Data
				d = new Data(Integer.parseInt(d_id), tbl_id, v_id, p.getPartition_id(), p.getPartition_serverId());
				d.setData_uid(null);
				
				// Set Partition, and Server id for the Virtual Data Node
				v.setVdata_partition_id(p.getPartition_id());
				v.setVdata_server_id(p.getPartition_serverId());
				
				// Add the corresponding Data id into the Virtual Data Node
				v.getVdata_set().add(d.getData_id());
				
				// Assign Data to Partition
				p.getPartition_dataSet().put(d.getData_id(), d);
				
			} else {
				
				d = new Data(Integer.parseInt(d_id), tbl_id, -1, p.getPartition_id(), p.getPartition_serverId());				
				
				// Assign Data to Partition
				p.getPartition_dataSet().put(d.getData_id(), d);
			}
			
			// Insert into data map to create index
			if(this.getData_map().containsKey(p_id))
				this.getData_map().get(p_id).add(d.getData_id());
			else {
				HashSet<Integer> dataSet = new HashSet<Integer>();
				dataSet.add(d.getData_id());
				this.getData_map().put(p_id, dataSet);
			}
			
			// Update Server statistic
			s = this.getServer(p.getPartition_serverId());
			s.incServer_totalData();
			
			replicas[repl - 1] = d.getData_id();
		}
		
		// Randomly returns a replica
		int rand_replica = Global.rand.nextInt(Global.replicas);
		return replicas[rand_replica];
	}
	
//====================================================================================================
	// Delete a Data and all of its replicas from the Cluster
	public void deleteDataRangePartitioning(int _id) {

		--Global.global_dataCount;
		
		Data d = null;
		Partition p = null;
		Server s = null;
		
		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = this.breakDataId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		for(int repl = 1; repl <= Global.replicas; repl++) {
			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
			
			if(Global.compressionBeforeSetup) {
				int v_id = Utility.simpleHash(tpl_pk, Global.virtualDataNodes);				
				int p_id = this.getVdata_pid_map().get(v_id);

				// Find the corresponding Partition from the Consistent Hash Ring
				p = this.getPartition(p_id);
				
				VirtualData v = this.vdataSet.get(v_id);
				v.getVdata_set().remove(Integer.parseInt(d_id));
				
			} else {				
				// Find the corresponding Partition from the Consistent Hash Ring
				int p_id = this.dataPartitionId(Integer.parseInt(d_id));
				p = this.getPartition(p_id);
			}
			
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
	
//====================================================================================================	
	// Insert a new Data and its replicas in the Cluster
	public int insertDataConsistentHash(int _id) {
		
		++Global.global_dataCount;
		
		int[] replicas = new int[Global.replicas];
		
		Data d = null;
		VirtualData v = null;		
		Partition p = null;
		Server s = null;

		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = this.breakDataId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		// Create a new Data object and its replicas
		for(int repl = 1; repl <= Global.replicas; repl++) {
			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
						
			if(Global.compressionBeforeSetup) {
				
				String v_uid = null;
				
				int v_id = Utility.simpleHash(tpl_pk, Global.virtualDataNodes);
				
				// Create a Virtual Data if required
				if(!this.vdataSet.containsKey(v_id)) {
					
					v_uid = Utility.getRandomAlphanumericString();						
					vdata_uid_map.put(v_id, v_uid);
					
					v = new VirtualData(v_id, v_uid);
					this.vdataSet.put(v_id, v);
					
				} else {
					v = this.vdataSet.get(v_id);
					v_uid = this.vdata_uid_map.get(v_id);
				}
				
				// Find the corresponding Partition from the Consistent Hash Ring
				p = this.getPartition(v_uid);
				
				// Create the new Data
				d = new Data(Integer.parseInt(d_id), tbl_id, v_id, p.getPartition_id(), p.getPartition_serverId());
				d.setData_uid(v_uid);
				
				// Set Partition, and Server id for the Virtual Data Node
				v.setVdata_partition_id(p.getPartition_id());
				v.setVdata_server_id(p.getPartition_serverId());
				
				// Add the corresponding Data id into the Virtual Data Node
				v.getVdata_set().add(d.getData_id());
				
				// Assign Data to Partition
				p.getPartition_dataSet().put(d.getData_id(), d);
				
			} else {
				
				String _uid = Utility.getRandomAlphanumericString();						
				data_uid_map.put(Integer.parseInt(d_id), _uid);
				
				// Find the corresponding Partition from the Consistent Hash Ring
				p = this.getPartition(_uid);			
				
				d = new Data(Integer.parseInt(d_id), tbl_id, -1, p.getPartition_id(), p.getPartition_serverId());				
				d.setData_uid(_uid);
				
				// Assign Data to Partition
				p.getPartition_dataSet().put(d.getData_id(), d);
			}
			
			// Update Server statistic
			s = this.getServer(p.getPartition_serverId());
			s.incServer_totalData();
			
			replicas[repl - 1] = d.getData_id();
		}
		
		// Randomly returns a replica
		int rand_replica = Global.rand.nextInt(Global.replicas);
		return replicas[rand_replica];
	}
	
//====================================================================================================
	// Delete a Data and all of its replicas from the Cluster | here _id = tpl_pk+tbl_id
	public void deleteDataConsistentHashing(int _id) {

		--Global.global_dataCount;
		
		Data d = null;
		Partition p = null;
		Server s = null;
		
		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = this.breakDataId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		for(int repl = 1; repl <= Global.replicas; repl++) {
			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
			
			if(Global.compressionBeforeSetup) {
				int v_id = Utility.simpleHash(tpl_pk, Global.virtualDataNodes);				
				String v_uid = this.getVdata_uid_map().get(v_id);

				// Find the corresponding Partition from the Consistent Hash Ring
				p = this.getPartition(v_uid);
				
				VirtualData v = this.vdataSet.get(v_id);
				v.getVdata_set().remove(Integer.parseInt(d_id));
				
			} else {				
				// Find the corresponding Partition from the Consistent Hash Ring
				String _uid = data_uid_map.get(Integer.parseInt(d_id));
				p = this.getPartition(_uid);
			}						
			
			// Delete from the data uid map
			this.data_uid_map.remove(Integer.parseInt(d_id));
			
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

//====================================================================================================
	private int dataPartitionId(int d_id) {
		for(Entry<Integer, HashSet<Integer>> entry : this.getData_map().entrySet()) {
			for(Integer d : entry.getValue())
				if(d == d_id)
					return entry.getKey();
		}
		
		return 0;
	}	

	//====================================================================================================	
	// Returns a Data object by its id
	public Data getData(int d_id) {
		
		Partition p = null;
		
		switch(Global.setup) {
			case "range":
				if(Global.compressionBeforeSetup) {
					// Break up the Data id to extract the Tuple's primary key and Table id
					String[] parts = Cluster.getTplIdFromDataId(d_id);
					int tpl_pk = Integer.parseInt(parts[0]);
					
					int v_id = Utility.simpleHash(tpl_pk, Global.virtualDataNodes);
					int p_id = this.getVdata_pid_map().get(v_id);
					p = this.getPartition(p_id);
					
				} else {
					int p_id = this.dataPartitionId(d_id);
					p = this.getPartition(p_id);
				}
				
				break;
		
			case "consistenthash":
				if(Global.compressionBeforeSetup) {	
					// Break up the Data id to extract the Tuple's primary key and Table id
					String[] parts = Cluster.getTplIdFromDataId(d_id);
					int tpl_pk = Integer.parseInt(parts[0]);
					
					int v_id = Utility.simpleHash(tpl_pk, Global.virtualDataNodes);				
					String v_uid = this.getVdata_uid_map().get(v_id);

					// Find the corresponding Partition from the Consistent Hash Ring
					p = this.getPartition(v_uid);			
								
				} else {			
					// Find the corresponding Partition from the Consistent Hash Ring
					String _uid = this.data_uid_map.get(d_id);
					p = this.getPartition(_uid);
				}

				break;
			
			default:
				Global.LOGGER.error("Wrong cluster setup method is specified !!! Choose either 'range' or 'consistenthash'");
				break;
		}
				
		return p.getData(this, d_id);
	}
	
	// Returns the reference of a Partition from the uid
	private Partition getPartition(String _uid) {
		
		// Find the corresponding Partition from the Consistent Hash Ring
		//hash = Utility.md5Hash(_uid);
		long hash = Utility.sha512Hash(_uid);				
		long p_key = this.getRing().get(hash);
		int p_id = this.getRing_map().get(p_key);
		
		return this.getPartition(p_id);
	}
	
	// Returns a Partition by its id
	public Partition getPartition(int id) {
		for(Partition p : this.getPartitions())
			if(p.getPartition_id() == id)
				return p;
		
		return null;
	}
	
	// Constructs data id with Tupple id, Table id, and Replica id
	// Unused
	public int getDataIdFromTupleId(int tpl_id) {
		
		String[] parts = this.breakDataId(tpl_id);
		
		int d_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		// Randomly returns a replica
		int randRepl = Global.rand.nextInt(Global.replicas) + 1;
		return Integer.parseInt((Integer.toString(d_pk)+Integer.toString(randRepl)+Integer.toString(tbl_id)));
	}
	
	// Unused
	public int inPartition(int x) {
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
			this.assignPartitionConsistentHash(p);
	}
	
	private void assignPartitionConsistentHash(Partition p) {
		int s_id = 0;
		
		// Assign Server id to the Partition
		s_id = (p.getPartition_id() % Global.servers)+1;
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
	
	// Extract actual Tuple id and Table id from a given Data id
	public String[] breakDataId(int _id) {
		String[] parts = new String[2];
		
		// Extract the last part from tuple id
		String d_id = Integer.toString(_id);
		int length = d_id.length();
		
		// Extract primary key and table id from the tuple id string
		parts[0] = StringUtils.substring(d_id, 0, (length - 1));
		parts[1] = StringUtils.substring(d_id, (length - 1), length);		
		
		return parts;
	}
	
	// Extract actual Tuple id, Replica id, and Table id from a given Data id
	// Only applicable where number of replicas and tables are less than 10
	public static String[] getTplIdFromDataId(int _id) {
		String[] parts = new String[3];
		
		// Extract the last part from tuple id
		String d_id = Integer.toString(_id);
		int length = d_id.length();
		
		// Extract primary key and table id from the tuple id string
		parts[0] = StringUtils.substring(d_id, 0, (length - 2));
		parts[1] = StringUtils.substring(d_id, (length - 2), (length - 1));		
		parts[2] = StringUtils.substring(d_id, (length - 1), length);
		
		return parts;
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