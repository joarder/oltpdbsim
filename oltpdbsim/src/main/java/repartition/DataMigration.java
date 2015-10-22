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

package main.java.repartition;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.CompressedData;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.dsm.FCICluster;
import main.java.entry.Global;
import main.java.metric.Metric;
import main.java.utils.IntPair;
import main.java.utils.Matrix;
import main.java.utils.MatrixElement;
import main.java.utils.Utility;
import main.java.utils.graph.ISimpleHypergraph;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.WorkloadBatch;
import main.java.workload.WorkloadExecutor;

public class DataMigration {
	public static int intra_server_dmgr = 0;
	public static int inter_server_dmgr = 0;	

	private static void setEnvironment(Cluster cluster) {
		intra_server_dmgr = 0;
		inter_server_dmgr = 0;
		
		Metric.reInitServerSet();		
				
		for(Server server : cluster.getServers()) {
			server.setServer_inflow(0);
			server.setServer_outflow(0);
		}
		
		for(Partition partition : cluster.getPartitions()) {			
			partition.setPartition_inflow(0);
			partition.setPartition_outflow(0);
		}
	}
	
	public static void performDataMigration(Cluster cluster, WorkloadBatch wb) {
		
		String partitioner = Global.workloadRepresentation;
		migratedDataSet = new HashSet<Data>();
		
		switch(Global.dataMigrationStrategy) {
			case "random":
				Global.LOGGER.info("Applying Random Cluster-to-Partition (Random) Strategy ...");				
				baseRandom(cluster, wb, partitioner);				
				break;
				
			case "mc":
				Global.LOGGER.info("Applying Max Column (MC) Strategy ...");
				strategyMC(cluster, wb, partitioner);							
				break;
			
			case "improved_mc":
				Global.LOGGER.info("Applying Improved Max Column (MC) Strategy ...");
				strategyImprovedMC(cluster, wb, partitioner);							
				break;
				
			case "msm":
				Global.LOGGER.info("Applying Max Sub Matrix (MSM) Strategy ...");
				strategyMSM(cluster, wb, partitioner);				
				break;
				
			case "rbsta":
				Global.LOGGER.info("Applying Repartitioning Based on Server-level Transactional Association (RBSTA) Strategy ...");
				strategyRBSTA(cluster, wb, partitioner);				
				break;				
				
			case "rbpta":
				Global.LOGGER.info("Applying Repartitioning Based on Partition-level Transactional Association (RBPTA) Strategy ...");
				strategyRBPTA(cluster, wb);		
				break;
				
			case "sword":
				Global.LOGGER.info("Applying Sword Strategy (SWD) ...");
				strategySword(cluster, wb, "hgr");				
				break;				
		}
	}
	
	private static void message() {
		Global.LOGGER.info("Generating Data Movement Mapping Matrix ...\n" +
				"      (Row :: Pre-Partition Id, Col :: Cluster Id, Elements :: Data Occurrence Counts)");
	}
	
	private static void baseRandom(Cluster cluster, WorkloadBatch wb, String partitioner) {
		setEnvironment(cluster);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cluster, wb);
		
		message();
		mapping.print();
		
		// Random assignment of which Cluster will go to which Partition
		int[] arr = new int[mapping.getN()];
		for (int i = 1; i <= arr.length-1; i++) {
		    arr[i] = i;
		}
		
		//System.out.println(">> "+mapping.getN()+"|"+arr.length);		
		if(!Global.swordInitial)
			Utility.shuffleArray(arr);
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();		
		for(int col = 0; col < mapping.getN(); col++) {
			// which cluster will go to which partition
			if(col == 0)
				keyMap.put(col, col);
			
			keyMap.put(col, arr[col]);
			//System.out.println("-#-Entry("+col+") [ACT] C"+col+"|P"+arr[col]);
		}
		
		// Perform Actual Data Movement
		if(Global.associative)
			migrate(cluster, wb, Global.dsm.hgr, keyMap, partitioner);
		else
			migrate(cluster, wb, wb.hgr, keyMap, partitioner);
	}
	
	private static void strategyMC(Cluster cluster, WorkloadBatch wb, String partitioner) {
		setEnvironment(cluster);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cluster, wb);
		
		message();
		mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();
		MatrixElement colMax;
		for(int col = 1; col < mapping.getN(); col++) {
			colMax = mapping.findColMax(col);
			
			// which cluster will go to which partition
			keyMap.put(colMax.getCol_pos(), colMax.getRow_pos());
			
			//System.out.println("-#-Col("+col+") [ACT] C"+(colMax.getCol_pos())+"|P"+(colMax.getRow_pos()));
		}
		
		// Perform Actual Data Movement
		if(Global.associative)
			migrate(cluster, wb, Global.dsm.hgr, keyMap, partitioner);
		else
			migrate(cluster, wb, wb.hgr, keyMap, partitioner);	
	}
	
	private static void strategyImprovedMC(Cluster cluster, WorkloadBatch wb, String partitioner) {
		setEnvironment(cluster);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cluster, wb);
		
		message();
		mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();
		Map<Integer, MatrixElement> colMaxSet; // Will hold a map containing partition id and max value matrix element
		
		for(int col = 1; col < mapping.getN(); col++) {
			// Get the Set of all Partitions having the max count in this Column
			colMaxSet = mapping.findColMaxSet(col);
			
			// Sort the Partitions based on their size
			Map<Integer, Integer> partitionSet = new HashMap<Integer, Integer>();
			
			if(Global.associative) {
				for(Entry<Integer, MatrixElement> entry : colMaxSet.entrySet()) {
					Server s = cluster.getServer(entry.getKey());
					int s_size = s.getServer_total_data();
					partitionSet.put(s.getServer_id(), s_size);
				}
				
			} else {
				for(Entry<Integer, MatrixElement> entry : colMaxSet.entrySet()) {
					Partition p = cluster.getPartition(entry.getKey());
					int p_size = p.getPartition_dataSet().size();
					partitionSet.put(p.getPartition_id(), p_size);
				}
			}
			
			// Sort by value in ascending order
			List<Entry<Integer, Integer>> sortedPartitionSet = Utility.sortedByValuesAsc(partitionSet);
			Entry<Integer, Integer> selectedPartition = sortedPartitionSet.get(0);
			
			// Select the Partition having lowest size
			MatrixElement colMax = colMaxSet.get(selectedPartition.getKey());
			
			keyMap.put(colMax.getCol_pos(), colMax.getRow_pos()); // which cluster will go to which partition
			//System.out.println("-#-Col("+col+") [ACT] C"+(colMax.getCol_pos())+"|P"+(colMax.getRow_pos()));
		}
		
		// Perform Actual Data Movement
		if(Global.associative)
			migrate(cluster, wb, Global.dsm.hgr, keyMap, partitioner);
		else
			migrate(cluster, wb, wb.hgr, keyMap, partitioner);	
	}
	
	private static void strategyMSM(Cluster cluster, WorkloadBatch wb, String partitioner) {	
		setEnvironment(cluster);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cluster, wb);
		message();
		mapping.print();
				
		// Step-1 :: Max Movement Matrix Formation
		MatrixElement max;
		int diagonal_pos = 1;		
		
		for(int m = 1; m < mapping.getM(); m++) {
			max = mapping.findMax(diagonal_pos);
			//System.out.println("[ACT] Max: "+max.getCounts()+", Col: "+(max.getCol_pos()+1)+", Row: "+(max.getRow_pos()+1));
			
			// Row/Col swap with diagonal Row/Col
			if(max.getValue() != 0) {
				mapping.swap_row(max.getRow_pos(), diagonal_pos);
				mapping.swap_col(max.getCol_pos(), diagonal_pos);
			}			
			
			++diagonal_pos;
		}		

		// @debug
		Global.LOGGER.info("Creating Movement Matrix after Sub Matrix Max calculation ...");
		mapping.print();
		
		// Step-2 :: PID Conversion		
		// Create the PID conversion Key Map
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>(); 
		for(int row = 1; row < mapping.getM(); row++) {
			keyMap.put((int)mapping.getMatrix()[0][row].getValue(), (int)mapping.getMatrix()[row][0].getValue());
			//System.out.println("-#-Row("+row+" [ACT] C"+(int)mapping.getMatrix()[0][row].getCounts()+"|P"+(int)mapping.getMatrix()[row][0].getCounts());
		}
	
		// Perform Actual Data Movement
		if(Global.associative)
			migrate(cluster, wb, Global.dsm.hgr, keyMap, partitioner);
		else
			migrate(cluster, wb, wb.hgr, keyMap, partitioner);
	}
	
	// RBSTA - incremental repartitioning	
	private static void strategyRBSTA(Cluster cluster, WorkloadBatch wb, String partitioner) {	
		setEnvironment(cluster);		
		RBSTA.populatePQ(cluster, wb);

		while(RBSTA.pq.size() != 0) {			
			// Get a transaction from the priority queue
			SimpleTr t = RBSTA.pq.poll();
			MigrationPlan m = t.migrationPlanList.get(0);
			
			System.out.println("--> "+t.toString());
			System.out.println("\t >> "+m.toString());
			
			// Check whether processing this transaction may increase the impact of any other already processed transactions
			if(!RBSTA.isAffected(wb, t, m))
				RBSTA.processTransaction(cluster, wb, t, m);
			else
				t.isProcessed = true;			
		}		
		
		wb.set_intra_dmv(intra_server_dmgr);
		wb.set_inter_dmv(inter_server_dmgr);
	}
		
	// FCIMining and ARHC - incremental repartitioning	
	public static void strategyARHC(Cluster cluster, WorkloadBatch wb) {	
		if(!WorkloadExecutor.isAdaptive)
			setEnvironment(cluster);		
		
		DataStreamMining.populatePQ(cluster, wb);

		while(!DataStreamMining.pq.isEmpty()) {			
			// Get a transaction from the priority queue
			SimpleTr t = DataStreamMining.pq.poll();
			MigrationPlan m = t.migrationPlanList.get(0);
			
			// Check whether processing this transaction may increase the impact of any other already processed transactions
			if(!DataStreamMining.isAffected(wb, t, m))
				DataStreamMining.processTransaction(cluster, wb, t, m);
			else
				t.isProcessed = true;			
		}		
		
		if(!Global.adaptive) {
			wb.set_intra_dmv(intra_server_dmgr);
			wb.set_inter_dmv(inter_server_dmgr);
		}
	}
	
	// Sword - incremental repartitioning	
	private static void strategySword(Cluster cluster, WorkloadBatch wb, String partitioner) {		
		if(Global.swordInitial) {
			// Initial data migration
			Global.LOGGER.info("Applying Random Cluster-to-Partition (Random) Strategy ...");
			migratedCDataSet = new HashSet<CompressedData>();
			baseRandom(cluster, wb, partitioner);
			
		} else {
			setEnvironment(cluster);		
			Sword.populatePQ(cluster, wb);
						
			Global.LOGGER.info("-----------------------------------------------------------------------------------------------------------------------");
			Global.LOGGER.info("Swapping candidate compressed vertices ...");
			
			int swaps = 0;
			while(!Sword.pq.isEmpty()) {
				//System.out.println(Sword.pq.poll().toString());
				SwordCHEdge sch = Sword.pq.peek();
				swaps += Sword.swapCandidatePair(cluster, wb, sch);				
				Sword.pq.remove();
			}
			
			Global.LOGGER.info("Total "+swaps+" compressed vertices have been swapped ...");
			
			wb.set_intra_dmv(intra_server_dmgr);
			wb.set_inter_dmv(inter_server_dmgr);
		}
	}
	
	// RBPTA - Swapping partitions
	private static void strategyRBPTA(Cluster cluster, WorkloadBatch wb) {
		setEnvironment(cluster);
		IntPair pSet = RBPTA.migrationDecision(cluster);
	
		if(pSet != null)
			swapPartitions(cluster, wb, pSet);			
		else
			Global.LOGGER.info("Partition swapping is not required or will not improve the situation at this moment !!!");
	}
	
	// RBPTA specific
	private static void swapPartitions(Cluster cluster, WorkloadBatch wb, IntPair pSet) {
		
		Global.LOGGER.info("-----------------------------------------------------------------------------------------------------------------------");
		Global.LOGGER.info("Selected Partitions for swapping: ");

		int Pa = pSet.x;
		int Pb = pSet.y;
		
		Partition P_a = cluster.getPartition(Pa);
		Partition P_b = cluster.getPartition(Pb);
		
		Global.LOGGER.info("\t\t"+P_a.toString());
		Global.LOGGER.info("\t\t"+P_b.toString());	
		
		int Sa = P_a.getPartition_serverId();
		int Sb = P_b.getPartition_serverId();
		
		// Swapping the partitions within the servers
		Server S_a = cluster.getServer(Sa);
		Server S_b = cluster.getServer(Sb);
				
		S_a.getServer_partitions().remove(Pa);
		S_b.getServer_partitions().remove(Pb);
		
		S_a.getServer_partitions().add(Pb);
		S_b.getServer_partitions().add(Pa);		
		
		// Change server id in partitions				
		P_a.setPartition_serverId(Sb);
		P_b.setPartition_serverId(Sa);
		
		// Change server id in data objects
		for(Entry<Integer, Data> entry : P_a.getPartition_dataSet().entrySet()) {
			entry.getValue().setData_server_id(Sb);
			++inter_server_dmgr;
			updateServerFlowCounts(cluster, Sa, Sb);
		}
		
		for(Entry<Integer, Data> entry : P_b.getPartition_dataSet().entrySet()) {
			entry.getValue().setData_server_id(Sa);
			++inter_server_dmgr;
			updateServerFlowCounts(cluster, Sb, Sa);
		}		
		
		// Update server-level load statistic and show
		cluster.updateLoad();
		//cluster.show();
		
		wb.set_intra_dmv(intra_server_dmgr);
		wb.set_inter_dmv(inter_server_dmgr);
	}
	
	// Updates Data
	private static void updateData(Cluster cluster, Data data, int dst_partition_id, int dst_server_id, 
			boolean roaming) {
		
		data.setData_partion_id(dst_partition_id);					
		data.setData_server_id(dst_server_id);
		
		if(roaming)
			data.setData_isRoaming(true);
		else
			data.setData_isRoaming(false);
	}		

	// Updates Partition
	private static void updatePartition(Cluster cluster, Data data, 
			int current_partition_id, int dst_partition_id) {
		
        Partition current_partition = cluster.getPartition(current_partition_id);
        Partition dst_partition = cluster.getPartition(dst_partition_id);
        Partition home_partition = cluster.getPartition(data.getData_homePartitionId());
        
        // Actual physical migration
        dst_partition.getPartition_dataSet().put(data.getData_id(), data);
        current_partition.getPartition_dataSet().remove(data.getData_id());
        
        // Update lookup table
        updateLookupTable(home_partition, data, dst_partition_id);
	}
	
	// Updates Partition lookup table
	private static void updateLookupTable(Partition home_partition, Data data, int dst_partition_id) {
		
		if(home_partition.getPartition_dataLookupTable().containsKey(data.getData_id())) {
			
        	home_partition.getPartition_dataLookupTable().remove(data.getData_id());
        	home_partition.getPartition_dataLookupTable().put(data.getData_id(), dst_partition_id);
        	
        } else {
        	
        	home_partition.getPartition_dataLookupTable().put(data.getData_id(), dst_partition_id);
        }
	}
	
	// Updates migration counts
	private static void updateMigrationCounts(Cluster cluster, int dst_server_id, 
			int src_server_id, int dst_partition_id, int current_partition_id) {
		
		cluster.getPartition(dst_partition_id).incPartition_inflow();		 
		cluster.getPartition(current_partition_id).incPartition_outflow();
		
		if(dst_server_id != src_server_id) {
			++inter_server_dmgr;
			
			updateServerFlowCounts(cluster, src_server_id, dst_server_id);
			updateMutuallyExclusiveServerSets(src_server_id, dst_server_id);
			
		} else {
			++intra_server_dmgr;
		}
	}		
	
	// Updates mutually exclusive server sets
	private static void updateMutuallyExclusiveServerSets(int src, int dst) {
		Metric.updateMESValue(src, dst);
	}
	
	// Updates server-level flow counts
	private static void updateServerFlowCounts(Cluster cluster, int src, int dst) {
		cluster.getServer(dst).incServer_totalData();
		cluster.getServer(src).decServer_totalData();
		
		cluster.getServer(dst).incServer_inflow();
		cluster.getServer(src).incServer_outflow();
	}	
	
	// Sword specific --  for the single static repartitioning
	static Set<CompressedData> migratedCDataSet;
	public static void migrateCompressedData(Cluster cluster, Data data,  
			int dst_partition_id, int dst_server_id) {
		
		CompressedData cd = cluster.getCDataMap().get(data.getData_compressed_data_id());
		
		if(!migratedCDataSet.contains(cd)) {
			migratedCDataSet.add(cd);
		
			cd.setCData_partition_id(dst_partition_id);
	    	cd.setCData_server_id(dst_server_id);
			
			// Move all the Data tuples residing in this compressed Data
	    	for(Integer d_id : cd.getCData_set()) {
	    		Data d = cluster.getData(d_id);    		
	    		migrateSingleData(cluster, d, dst_server_id, dst_partition_id);    		
	    	}
		}
	}
	
	// Migrates a single Data from one partition to another
	// Performs actual Data migration operation
	static Set<Data> migratedDataSet;
	static void migrateSingleData(Cluster cluster, Data data, int dst_server_id, int dst_partition_id) {
		
		if(!migratedDataSet.contains(data)) {
			migratedDataSet.add(data);
		
			Partition dst_partition = cluster.getPartition(dst_partition_id);
			Partition current_partition = cluster.getPartition(data.getData_partition_id());
			Partition home_partition = cluster.getPartition(data.getData_homePartitionId());												
			
			int current_server_id = data.getData_server_id();
			int current_partition_id = data.getData_partition_id();
			int home_partition_id = data.getData_homePartitionId();		
			
			if(dst_partition_id != current_partition_id) { // Data needs to be moved					
				if(data.isData_isRoaming()) { // Data is already Roaming
					if(dst_partition_id == home_partition_id) { 
						// Data will be roamed back to its home partition
						updateData(cluster, data, dst_partition_id, dst_server_id, false);
						updatePartition(cluster, data, current_partition_id, dst_partition_id);
						updateMigrationCounts(cluster, dst_server_id, current_server_id, dst_partition_id, current_partition_id);
						
						current_partition.decPartition_foreign_data();
						home_partition.decPartition_roaming_data();
						
					} else { 
						// Data will be roamed to another foreign partition
						updateData(cluster, data, dst_partition_id, dst_server_id, true);
						updatePartition(cluster, data, current_partition_id, dst_partition_id);
						updateMigrationCounts(cluster, dst_server_id, current_server_id, dst_partition_id, current_partition_id);
						
						dst_partition.incPartition_foreign_data();
						current_partition.decPartition_foreign_data();							
					}
				} else { 
					// Data will be roamed for its home partition
					updateData(cluster, data, dst_partition_id, dst_server_id, true);						
					updatePartition(cluster, data, current_partition_id, dst_partition_id);
					updateMigrationCounts(cluster, dst_server_id, current_server_id, dst_partition_id, current_partition_id);						
					
					dst_partition.incPartition_foreign_data();								
					home_partition.incPartition_roaming_data();
				}
			}	
		}
	}	
	
	// Migrates all the Data present in the current workload
	private static void migrate(Cluster cluster, WorkloadBatch wb, 
			ISimpleHypergraph<SimpleVertex, SimpleHEdge> hgr, Map<Integer, Integer> keyMap, String type) {

		Partition dst_partition = null;
		
		int current_partition_id = -1;
		int dst_partition_id = -1;		
		int current_server_id = -1;		
		int dst_server_id = -1;		
		
		Set<Integer> dataSet = new TreeSet<Integer>();			

		for(SimpleVertex v : hgr.getVertices()) {
			Data data = cluster.getData(v.getId());
			
			if(!dataSet.contains(data.getData_id()) && data.isData_inUse()) {
				dataSet.add(data.getData_id());																		
				
				current_partition_id = data.getData_partition_id();
				current_server_id = data.getData_server_id();			
				
				switch(type) {
				
					case "hgr":
						if(Global.compressionEnabled) {
							dst_partition_id = keyMap.get(data.getData_chmetisClusterId());
							data.setData_chmetisClusterId(-1);
							
						} else {
							dst_partition_id = keyMap.get(data.getData_hmetisClusterId());
							data.setData_hmetisClusterId(-1);
						}
						break;
												
					case "gr":
						if(Global.compressionEnabled) {
							dst_partition_id = keyMap.get(data.getData_chmetisClusterId());
							data.setData_chmetisClusterId(-1);
							
						} else {
							dst_partition_id = keyMap.get(data.getData_metisClusterId());
							data.setData_metisClusterId(-1);
						}
						
						break;
				}
				
				//System.out.println("@debug >> P"+dst_partition_id);
				
				if(Global.associative) {
					// Decide destination server
					dst_server_id = dst_partition_id;

					// Decide destination partition if data needs to be migrated						
					if(dst_server_id != current_server_id) {
						dst_partition_id = selectPartitionId(cluster, dst_server_id);
						dst_partition = cluster.getPartition(dst_partition_id);
						
					} else {
						// Target Data tuple is already in the destination server, no migration is required
						dst_partition_id = current_partition_id;
					}
					
					// Populate FCI clusters
					if(DataStreamMining.fci_clusters.containsKey(dst_server_id)) {
						DataStreamMining.fci_clusters.get(dst_server_id).fci.add(data.getData_id());
					} else {						
						DataStreamMining.fci_clusters.put(dst_server_id, new FCICluster());
						DataStreamMining.fci_clusters.get(dst_server_id).fci.add(data.getData_id());
					}

				} else {
					dst_partition = cluster.getPartition(dst_partition_id);
					dst_server_id = dst_partition.getPartition_serverId();
				}
				
				// Actual Data migration
				migrateSingleData(cluster, data, dst_server_id, dst_partition_id);
				
				// Sword specific -- Migrate the entire corresponding Compressed Data
				if(Global.compressionBeforeSetup && Global.swordInitial)
					DataMigration.migrateCompressedData(cluster, data, dst_partition_id, dst_server_id);
				
				data.setData_inUse(false);
				
			} // end -- if()-Data
		} // end -- for()-Data	
					
		wb.set_intra_dmv(intra_server_dmgr);
		wb.set_inter_dmv(inter_server_dmgr);	
	}
		
	private static int selectPartitionId(Cluster cluster, int server_id) {
		// Sort the Partitions based on their size	
		Map<Integer, Integer> partitionSet = new HashMap<Integer, Integer>();
		Server s = cluster.getServer(server_id);
		
		for(int p_id : s.getServer_partitions()) {
			Partition p = cluster.getPartition(p_id);
			int p_size = p.getPartition_dataSet().size();
			partitionSet.put(p.getPartition_id(), p_size);
		}
		
		// Sort by value in ascending order
		List<Entry<Integer, Integer>> sortedPartitionSet = Utility.sortedByValuesAsc(partitionSet);
		return sortedPartitionSet.get(0).getKey();
	}
}