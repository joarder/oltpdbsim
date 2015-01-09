/**
 * @author Joarder Kamal
 * 
 * Perform Data Movement after analysing Workload using HyperGraph Partitioning 
 */

package main.java.repartition;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.cluster.VirtualData;
import main.java.entry.Global;
import main.java.repartition.MappingTable;
import main.java.utils.Matrix;
import main.java.utils.MatrixElement;
import main.java.utils.graph.SimpleHEdge;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class DataMovement {
	private static int intra_server_dmv = 0;
	private static int inter_server_dmv = 0;	

	private static void setEnvironment(Cluster cluster) {
		intra_server_dmv = 0;
		inter_server_dmv = 0;		
				
		for(Server server : cluster.getServers()) {
			server.setServer_inflow(0);
			server.setServer_outflow(0);
		}
		
		for(Partition partition : cluster.getPartitions()) {			
			partition.setPartition_inflow(0);
			partition.setPartition_outflow(0);
		}
	}
	
	public static void performDataMovement(Cluster cluster, WorkloadBatch wb, String strategy, String partitioner) {
		switch(strategy) {
			case "random":
				Global.LOGGER.info("Applying Random Cluster-to-Partition Strategy (Random) ...");
				baseRandom(cluster, wb, partitioner);				
				break;
				
			case "mc":
				Global.LOGGER.info("Applying Max Column Strategy (MC) ...");
				strategyMC(cluster, wb, partitioner);							
				break;
				
			case "msm":
				Global.LOGGER.info("Applying Max Sub Matrix Strategy (MSM) ...");
				strategyMSM(cluster, wb, partitioner);				
				break;
				
			case "sword":
				Global.LOGGER.info("Applying Sword Strategy (SWD) ...");
				strategySword(cluster, wb, partitioner);				
				break;	
		}
	}
	
	private static void message() {
		Global.LOGGER.info("Generating Data Movement Mapping Matrix ...\n" +
				"      (Row :: Pre-Partition Id, Col :: Cluster Id, Elements :: Data Occurrence Counts)");
	}
	
	private static void shuffleArray(int[] array)
	{
	    int index, temp;	    
	    for (int i = array.length - 1; i > 1; i--)
	    {
	        index = Global.rand.nextInt(i-1) + 1;
	        temp = array[index];
	        array[index] = array[i];
	        array[i] = temp;
	    }
	}
	
	private static void baseRandom(Cluster cluster, WorkloadBatch wb, String partitioner) {
		setEnvironment(cluster);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cluster, wb);
		message();
		//mapping.print();
		
		// Random assignment		
		int[] arr = new int[mapping.getN()];
		for (int i = 1; i <= arr.length-1; i++) {
		    arr[i] = i;
		}
		
		//System.out.println(">> "+mapping.getN()+"|"+arr.length);		
		shuffleArray(arr);
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();		
		for(int col = 0; col < mapping.getN(); col++) {				
			//keyMap.put(col, col); // which cluster will go to which partition
			//System.out.println("-#-Entry("+col+") [ACT] C"+col+"|P"+col);
			
			if(col == 0)
				keyMap.put(col, col); // which cluster will go to which partition
			
			keyMap.put(col, arr[col]); // which cluster will go to which partition
			//System.out.println("-#-Entry("+col+") [ACT] C"+col+"|P"+arr[col]);
		}
		
		// Perform Actual Data Movement
		move(cluster, wb, keyMap, partitioner);
	}
	
	private static void strategyMC(Cluster cluster, WorkloadBatch wb, String partitioner) {
		setEnvironment(cluster);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cluster, wb);
		message();
		//mapping.print();
		
		// Create Key-Value (Destination PID-Cluster ID) Mappings from Mapping Matrix
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>();
		MatrixElement colMax;
		for(int col = 1; col < mapping.getN(); col++) {
			colMax = mapping.findColMax(col);
			keyMap.put(colMax.getCol_pos(), colMax.getRow_pos()); // which cluster will go to which partition
			//System.out.println("-#-Col("+col+") [ACT] C"+(colMax.getCol_pos())+"|P"+(colMax.getRow_pos()));
		}
		
		// Perform Actual Data Movement
		move(cluster, wb, keyMap, partitioner);	
	}
	
	private static void strategyMSM(Cluster cluster, WorkloadBatch wb, String partitioner) {	
		setEnvironment(cluster);
		
		// Create Mapping Matrix
		MappingTable mappingTable = new MappingTable();		
		Matrix mapping = mappingTable.generateMappingTable(cluster, wb);
		message();
		//mapping.print();
				
		// Step-1 :: Max Movement Matrix Formation
		MatrixElement max;
		int diagonal_pos = 1;		
		
		for(int m = 1; m < mapping.getM(); m++) {
			max = mapping.findMax(diagonal_pos);
			//System.out.println("[ACT] Max: "+max.getCounts()+", Col: "+(max.getCol_pos()+1)+", Row: "+(max.getRow_pos()+1));
			
			// Row/Col swap with diagonal Row/Col
			if(max.getCounts() != 0) {
				mapping.swap_row(max.getRow_pos(), diagonal_pos);
				mapping.swap_col(max.getCol_pos(), diagonal_pos);
			}			
			
			++diagonal_pos;
		}		

		// @debug
		Global.LOGGER.info("Creating Movement Matrix after Sub Matrix Max calculation ...");
		//mapping.print();
		
		// Step-2 :: PID Conversion		
		// Create the PID conversion Key Map
		Map<Integer, Integer> keyMap = new TreeMap<Integer, Integer>(); 
		for(int row = 1; row < mapping.getM(); row++) {
			keyMap.put((int)mapping.getMatrix()[0][row].getCounts(), (int)mapping.getMatrix()[row][0].getCounts());
			//System.out.println("-#-Row("+row+" [ACT] C"+(int)mapping.getMatrix()[0][row].getCounts()+"|P"+(int)mapping.getMatrix()[row][0].getCounts());
		}
	
		// Perform Actual Data Movement	
		move(cluster, wb, keyMap, partitioner);
	}
	
	// Sword - incremental repartitioning	
	private static void strategySword(Cluster cluster, WorkloadBatch wb, String partitioner) {
		setEnvironment(cluster);
		
		
	}
	
	private static void updateData(Cluster cluster, Data data, 
			int dst_partition_id, int dst_server_id, boolean roaming) {
		
		data.setData_partion_id(dst_partition_id);					
		data.setData_server_id(dst_server_id);
		
		if(roaming)
			data.setData_isRoaming(true);
		else
			data.setData_isRoaming(false);
		        
        // Only for Sword
        if(Global.compressionBeforeSetup) {
        	
        	VirtualData v = cluster.getVdataSet().get(data.getData_vdata_id());
        	v.setVdata_partition_id(dst_partition_id);
        	v.setVdata_server_id(dst_server_id);
        	
        	// Move all the Data objects reside in this Virtual Data
        	for(Integer d_id : v.getVdata_set()) {
        		Data d = cluster.getData(d_id);
        		int current_partition_id = d.getData_partition_id();
        		int current_server_id = d.getData_server_id();
        		
        		d.setData_partion_id(dst_partition_id);					
        		d.setData_server_id(dst_server_id);
        		
        		if(roaming)
        			d.setData_isRoaming(true);
        		else
        			d.setData_isRoaming(false);
        		
        		updatePartition(cluster, d, d.getData_partition_id(), dst_partition_id);
        		updateMovementCounts(cluster, dst_server_id, current_server_id, 
        				dst_partition_id, current_partition_id);
        	}
        }
	}
	
	private static void updatePartition(Cluster cluster, Data data, 
			int current_partition_id, int dst_partition_id) {
		
        Partition current_partition = cluster.getPartition(current_partition_id);
        Partition dst_partition = cluster.getPartition(dst_partition_id);
        Partition home_partition = cluster.getPartition(data.getData_homePartitionId());
        
        // Actual Movement
        //dst_partition.getPartition_dataSet().add(data);
        dst_partition.getPartition_dataSet().put(data.getData_id(), data);
        current_partition.getPartition_dataSet().remove(data.getData_id());
        
        // Update Lookup Table
        updateLookupTable(home_partition, dst_partition_id, data);
	}
	
	private static void updateLookupTable(Partition home_partition, int dst_partition_id, 
			Data data) {
		
		if(home_partition.getPartition_dataLookupTable().containsKey(data.getData_id())) {
			
        	home_partition.getPartition_dataLookupTable().remove(data.getData_id());
        	home_partition.getPartition_dataLookupTable().put(data.getData_id(), dst_partition_id);
        	
        } else {
        	
        	home_partition.getPartition_dataLookupTable().put(data.getData_id(), dst_partition_id);
        }
	}
	
	private static void updateMovementCounts(Cluster cluster, int dst_server_id, 
			int current_server_id, int dst_partition_id, int current_partition_id) {
		
		cluster.getPartition(dst_partition_id).incPartition_inflow();		 
		cluster.getPartition(current_partition_id).incPartition_outflow();
		
		if(dst_server_id != current_server_id) {
			++inter_server_dmv;
			
			cluster.getServer(dst_server_id).incServer_totalData();
			cluster.getServer(current_server_id).decServer_totalData();
			
			cluster.getServer(dst_server_id).incServer_inflow();
			cluster.getServer(current_server_id).incServer_outflow();
			
		} else
			++intra_server_dmv;		
	}		
	
	// Perform Actual Data Movement
	private static void move(Cluster cluster, WorkloadBatch wb, Map<Integer, Integer> keyMap, 
			String type) {
		
		Partition home_partition = null;
		Partition current_partition = null;
		Partition dst_partition = null;
		
		int home_partition_id = -1;
		int current_partition_id = -1;
		int dst_partition_id = -1;		
		int current_server_id = -1;		
		int dst_server_id = -1;		
		
		Set<Integer> dataSet = new TreeSet<Integer>();			
						
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			
			Transaction tr = wb.getTransaction(h.getId());
		
			for(Integer d : tr.getTr_dataSet()) {
				
				Data data = cluster.getData(d);
				
				if(!dataSet.contains(data.getData_id()) && data.isData_inUse()) {
					dataSet.add(data.getData_id());
					
					home_partition_id = data.getData_homePartitionId();
					home_partition = cluster.getPartition(data.getData_homePartitionId());																		
					
					current_partition_id = data.getData_partition_id();									
					current_partition = cluster.getPartition(current_partition_id);
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
							dst_partition_id = keyMap.get(data.getData_metisClusterId());
							data.setData_metisClusterId(-1);
							break;
					}
					
					//System.out.println("@debug >> P"+dst_partition_id);
					dst_partition = cluster.getPartition(dst_partition_id);
					dst_server_id = dst_partition.getPartition_serverId();												
					
					if(dst_partition_id != current_partition_id) { // Data needs to be moved					
						if(data.isData_isRoaming()) { // Data is already Roaming
							if(dst_partition_id == home_partition_id) {
								updateData(cluster, data, dst_partition_id, dst_server_id, false);
								updatePartition(cluster, data, current_partition_id, dst_partition_id);
								updateMovementCounts(cluster, dst_server_id, current_server_id, dst_partition_id, current_partition_id);																		
								
								current_partition.decPartition_foreign_data();
								home_partition.decPartition_roaming_data();
								
							} else if(dst_partition_id == current_partition_id) {									
								// Nothing to do									
							} else {
								updateData(cluster, data, dst_partition_id, dst_server_id, true);
								updatePartition(cluster, data, current_partition_id, dst_partition_id);
								updateMovementCounts(cluster, dst_server_id, current_server_id, dst_partition_id, current_partition_id);
								
								dst_partition.incPartition_foreign_data();
								current_partition.decPartition_foreign_data();
								
							}
						} else {
							updateData(cluster, data, dst_partition_id, dst_server_id, true);
							updatePartition(cluster, data, current_partition_id, dst_partition_id);
							updateMovementCounts(cluster, dst_server_id, current_server_id, dst_partition_id, current_partition_id);
							
							dst_partition.incPartition_foreign_data();								
							home_partition.incPartition_roaming_data();
						}
					}
					
					data.setData_inUse(false);
				} // end -- if()-Data
			} // end -- for()-Data
			
			if(tr.isDt() && Global.compressionBeforeSetup)
				wb.sword.hCut.add(h);
			
		} // end -- for()		
		
		wb.set_intra_dmv(intra_server_dmv);
		wb.set_inter_dmv(inter_server_dmv);	
	}
}