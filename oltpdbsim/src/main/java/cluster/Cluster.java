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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import main.java.db.Database;
import main.java.db.Table;
import main.java.db.Tuple;
import main.java.entry.Global;
import main.java.metric.Metric;
import main.java.utils.Utility;
import main.java.workload.Workload;
import main.java.workload.WorkloadBatch;
import main.java.workload.WorkloadConstants;

import org.apache.commons.lang3.StringUtils;

public class Cluster {	
	private SortedSet<Server> servers;
	private SortedSet<Partition> partitions;
	private Set<Integer> serverSet;
	
	private Map<Integer, ArrayList<Integer>> partition_map;
	private Map<Integer, HashSet<Integer>> data_map;	
	private Map<Integer, CompressedData> cData_map;
	
	private ConsistentHashRing<Long> cluster_ring;
	private Map<Long, Integer> ring_map;
	private Map<Integer, ArrayList<Long>> partition_keyRange;
	
	private Map<Integer, String> data_uid_map;
	private Map<Integer, String> cData_uid_map;
	
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
		this.serverSet = new HashSet<Integer>();
		this.setPartition_map(new HashMap<Integer, ArrayList<Integer>>());
		
		if(Global.compressionBeforeSetup) {
			this.setCDataMap(new HashMap<Integer, CompressedData>());
			this.setCData_uid_map(new HashMap<Integer, String>());
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

	public Set<Integer> getServerSet() {
		return serverSet;
	}

	public void setServerSet(Set<Integer> serverSet) {
		this.serverSet = serverSet;
	}

	public Map<Integer, CompressedData> getCDataMap() {
		return cData_map;
	}

	public void setCDataMap(Map<Integer, CompressedData> cDataSet) {
		this.cData_map = cDataSet;
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

	public Map<Integer, String> getCData_uid_map() {
		return cData_uid_map;
	}

	public void setCData_uid_map(Map<Integer, String> cData_uid_map) {
		this.cData_uid_map = cData_uid_map;
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
		WorkloadBatch wb = null;
		
		Global.LOGGER.info("Setting up Cluster ...");
		
		// Add Partitions
		int db_tables = db.getDb_tables().size();
		int range_partitions = db_tables * Global.servers;
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Creating "+range_partitions/db_tables+" logical Partitions for each range partitioned database table ...");
	    Global.LOGGER.info("Creating a total "+range_partitions+" logical Partitions for "+db.getDb_tables().size()+" database tables ...");
	    
		for(int i = 1; i <= range_partitions; i++)
			this.getPartitions().add(new Partition(i));
		
		// Add Servers and fixed amount of Partitions
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Adding "+Global.servers+" physical Servers in the Cluster ...");
		
		for(int i = 1; i <= Global.servers; i++)
			this.addServer(new Server(i), true);			
		
		// New - mutually exclusive pairwise server sets
		Metric.initServerSet(this);
		
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
			
			int ssd_id = this.getPartitionSSD(s);
			p.setPartition_server_ssd_id(ssd_id);
			
			s.getServer_SSDs().get(ssd_id).getSSD_partitions().add(p.getPartition_id());			
			s.getServer_partitions().add(p.getPartition_id());
			
			// Add an entry in the Cluster's Server-Partition Mapping Table
			this.addPartitionMappingEntry(s_id, p.getPartition_id());
			
			Table tbl = db.getTable(tbl_id);
			Global.LOGGER.info("Partition "+p.getPartition_label()+" of Table '"+tbl.getTbl_name()+"' is assigned to Server "+s.getServer_label()+" in SSD"+ssd_id+".");
			
			++s_id;			
			if(s_id > Global.servers) {
				s_id = 1;
				++tbl_id;
			}			
		}
		
		// Determine the number of Compressed Data Nodes to be created
		if(Global.compressionEnabled)
			Global.compressedVertices = ((int) db.getDb_tuple_counts() / (int) Global.compressionRatio);
				
		// Physical Data Distribution
		this.physicalDataDistribution(db);	
		
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

		// New - mutually exclusive pairwise server sets
		Metric.initServerSet(this);			
		
		// Assign Partitions into Servers
		Global.LOGGER.info("-----------------------------------------------------------------------------");
		Global.LOGGER.info("Assigning logical Partitions into physical Servers ...");
		
		long p_uid = -1;
		long p_min = Long.MIN_VALUE;
		long p_max = Long.MAX_VALUE; //Long.MAX_VALUE; //1048576L; // 2^20 //1073741824; // 2^30
		long p_size = (long)((p_max/Global.partitions)+1)*2;
		
		ArrayList<Long> p_keyRange = null;		
		Global.partitionCapacity = p_size;
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
		
		// Determine the number of compressed data nodes to be created
		if(Global.compressionEnabled)
			Global.compressedVertices = ((int) db.getDb_tuple_counts() / (int) Global.compressionRatio);
				
		// Physical Data Distribution
		this.physicalDataDistribution(db);
		
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
		for(int p_id : s.getServer_partitions()) {			
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

				if(!tpl.getValue().getTuple_action().equals(WorkloadConstants.TPL_INSERT)) {
					
					switch(Global.setup) {
						case "range":
							int tbl_id = tbl.getTbl_id();
							
							++s_id;
							if(s_id > Global.servers)
								s_id = 1;
							
							Server s = this.getServer(s_id);
							int p_id = this.getRangePartition(s, tbl_id);
							this.insertData_RangePartitioning(tpl.getValue().getTuple_id(), s_id, p_id);
							
							break;
						
						case "consistenthash":
							this.insertData_ConsistentHash(tpl.getValue().getTuple_id());
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
	public int insertData_RangePartitioning(int _id, int s_id, int p_id) {
		++Global.global_dataCount;
		
		int[] replicas = new int[Global.replicas];
		int cData_id = -1;
		
		Data d = null;

		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = breakDataIdWithoutReplicaId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		Server s = this.getServer(s_id);
		Partition p = this.getPartition(p_id);
		
		// Create a new Data object and its replicas
		for(int repl = 1; repl <= Global.replicas; repl++) {			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
			
			// Create the new Data
			d = new Data(Integer.parseInt(d_id), tbl_id, cData_id, p.getPartition_id(), p.getPartition_serverId());
			d.setData_uid(null);
			
			// Assign Data to Partition
			p.getPartition_dataSet().put(d.getData_id(), d);
			
			if(Global.compressionEnabled)
				cData_id = Utility.simpleHash(tpl_pk, Global.compressedVertices);
			
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
		String[] parts = breakDataIdWithoutReplicaId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		for(int repl = 1; repl <= Global.replicas; repl++) {			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
			
			// Find the corresponding Partition from the lookup table
			int p_id = this.getDataPartitionId(Integer.parseInt(d_id));
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
	
//====================================================================================================	
	// Insert a new Data and its replicas in the Cluster
	public int insertData_ConsistentHash(int _id) {
		
		++Global.global_dataCount;
		
		int[] replicas = new int[Global.replicas];
		int cData_id = -1;
		
		Data d = null;
		CompressedData c = null;		
		Partition p = null;
		Server s = null;

		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = breakDataIdWithoutReplicaId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		// Create a new Data object and its replicas
		for(int repl = 1; repl <= Global.replicas; repl++) {
			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
			
			if(Global.compressionEnabled)
				cData_id = Utility.simpleHash(tpl_pk, Global.compressedVertices);
						
			if(Global.compressionBeforeSetup) {				
				String c_uid = null;				

				// Create a compressed data if required
				if(!this.getCDataMap().containsKey(cData_id)) {
					
					c_uid = Utility.getRandomAlphanumericString();						
					cData_uid_map.put(cData_id, c_uid);
					
					c = new CompressedData(cData_id, c_uid);
					this.getCDataMap().put(cData_id, c);
					
				} else {
					c = this.getCDataMap().get(cData_id);
					c_uid = this.cData_uid_map.get(cData_id);
				}
				
				// Find the corresponding Partition from the Consistent Hash Ring
				p = this.getPartition(c_uid);
				
				// Create the new Data
				d = new Data(Integer.parseInt(d_id), tbl_id, cData_id, p.getPartition_id(), p.getPartition_serverId());
				d.setData_uid(c_uid);
				
				// Set Partition, and Server id for the compressed data
				c.setCData_partition_id(p.getPartition_id());
				c.setCData_server_id(p.getPartition_serverId());
				
				// Add the corresponding Data id into the compressed data
				c.getCData_set().add(d.getData_id());
				
				// Assign Data to Partition
				p.getPartition_dataSet().put(d.getData_id(), d);
				
			} else {
				
				String _uid = Utility.getRandomAlphanumericString();						
				data_uid_map.put(Integer.parseInt(d_id), _uid);
				
				// Find the corresponding Partition from the Consistent Hash Ring
				p = this.getPartition(_uid);			
				
				d = new Data(Integer.parseInt(d_id), tbl_id, cData_id, p.getPartition_id(), p.getPartition_serverId());				
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
		String[] parts = breakDataIdWithoutReplicaId(_id);
		int tpl_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		for(int repl = 1; repl <= Global.replicas; repl++) {
			
			String d_id = Integer.toString(tpl_pk)+Integer.toString(repl)+Integer.toString(tbl_id);
			
			if(Global.compressionBeforeSetup) {
				int cd_id = Utility.simpleHash(tpl_pk, Global.compressedVertices);				
				String cd_uid = this.getCData_uid_map().get(cd_id);

				// Find the corresponding Partition from the Consistent Hash Ring
				p = this.getPartition(cd_uid);
				
				CompressedData cd = this.getCDataMap().get(cd_id);
				cd.getCData_set().remove(Integer.parseInt(d_id));
				
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
	private int getDataPartitionId(int d_id) {
		for(Entry<Integer, HashSet<Integer>> entry : this.getData_map().entrySet())
			if(entry.getValue().contains(d_id))			
				return entry.getKey();		
		
		return -1;
	}	

	//====================================================================================================	
	// Returns a Data object by its id
	public Data getData(int d_id) {
		
		Partition p = null;
		
		// Break up the Data id to extract the Tuple's primary key and Table id
		String[] parts = Cluster.breakDataIdWithReplicaId(d_id);
		int tpl_pk = Integer.parseInt(parts[0]);		
		
		switch(Global.setup) {
			case "range":
				int p_id = this.getDataPartitionId(d_id);
				p = this.getPartition(p_id);				
				
				break;
		
			case "consistenthash":
				if(Global.compressionBeforeSetup) {
					int cData_id = Utility.simpleHash(tpl_pk, Global.compressedVertices);				
					String cData_uid = this.getCData_uid_map().get(cData_id);

					// Find the corresponding Partition from the Consistent Hash Ring
					p = this.getPartition(cData_uid);			
								
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
		
	// Update statistic for the Servers
	public void updateLoad() {
		for(Server s : this.getServers())
			s.updateServer_load(this);
		
		//for(Partition p : this.getPartitions())
			//p.updatePartition_load();
	}
	
	// Currently only supports initial add
	public void addServer(Server s, boolean init) {		
		this.getServers().add(s);
		this.getServerSet().add(s.getServer_id());
		
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
	
	private int getPartitionSSD(Server s) {
		int ssd_id = s.getServer_last_assigned_ssd();
		++ssd_id;
		
		if(ssd_id > Global.serverSSD)
			ssd_id = 1;
		
		s.setServer_last_assigned_ssd(ssd_id);
		
		return ssd_id;
	}
	
	private void assignPartitionConsistentHash(Partition p) {		
		// Assign Server id to the Partition
		int s_id = (p.getPartition_id() % Global.servers)+1;
		p.setPartition_serverId(s_id);
		
		// Add Partition id in the corresponding Server's list 
		Server s = this.getServer(s_id);
		
		int ssd_id = this.getPartitionSSD(s);
		p.setPartition_server_ssd_id(ssd_id);
		
		s.getServer_SSDs().get(ssd_id).getSSD_partitions().add(p.getPartition_id());		
		s.getServer_partitions().add(p.getPartition_id());
		
		// Add an entry in the Cluster's Server-Partition Mapping Table
		this.addPartitionMappingEntry(s_id, p.getPartition_id());
		
		Global.LOGGER.info("Partition "+p.getPartition_label()+" is assigned to Server "+s.getServer_label()+" in SSD"+ssd_id+".");
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
		
		Global.LOGGER.info("Partition P"+p_id+" is assigned to Server S"+s_id+".");
		Global.LOGGER.info("Server-Partition mapping in the central lookup table has been updated.");		
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
	
	// Extract actual Tuple primary key and corresponding Table id from a given Data id without its replica id
	public static String[] breakDataIdWithoutReplicaId(int _id) {
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
	public static String[] breakDataIdWithReplicaId(int _id) {
		String[] parts = new String[3];
		
		// Extract the last part from tuple id
		String d_id = Integer.toString(_id);
		int length = d_id.length();
		
		// Extract primary key and table id from the tuple id string
		parts[0] = StringUtils.substring(d_id, 0, (length - 2)); // Tuple Primary Key
		parts[1] = StringUtils.substring(d_id, (length - 2), (length - 1));	// Replica Id
		parts[2] = StringUtils.substring(d_id, (length - 1), length); // Table Id
		
		return parts;
	}
	
	// Constructs data id with Tupple id, Table id, and Replica id	
	public static int getDataIdFromTupleId(int tpl_id) {		
		String[] parts = breakDataIdWithoutReplicaId(tpl_id);
		
		int d_pk = Integer.parseInt(parts[0]);
		int tbl_id = Integer.parseInt(parts[1]);
		
		// Randomly returns a replica
		int randRepl = Global.rand.nextInt(Global.replicas) + 1;
		return Integer.parseInt((Integer.toString(d_pk)+Integer.toString(randRepl)+Integer.toString(tbl_id)));
	}
		
	public void show() {
		Global.LOGGER.info("<-- Cluster Status -->");
		Global.LOGGER.info("Number of Servers: "+this.getServers().size());
		Global.LOGGER.info("Number of Partitions: "+this.getPartitions().size());
		Global.LOGGER.info("Total data: "+Global.global_dataCount);
		
		// Server Details
		for(Server s : this.getServers()) {						
			Global.LOGGER.info("--"+s.toString());
			s.show(this);
		}		
		
		Global.LOGGER.info("-----------------------------------------------------------------------------");
	}
}