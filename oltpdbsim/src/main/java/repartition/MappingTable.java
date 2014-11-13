/**
 * @author Joarder Kamal
 */

package main.java.repartition;

import java.util.Set;
import java.util.TreeSet;
import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.utils.Matrix;
import main.java.utils.MatrixElement;
import main.java.utils.graph.SimpleHEdge;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class MappingTable {
	public MappingTable() {}
	
	public Matrix generateMappingTable(Cluster cluster, WorkloadBatch wb) {
		int M = cluster.getPartitions().size()+1;
		int N = M; // Having a NxN matrix
		
		// Create a 2D Matrix to represent the Data movements due to partitioning decision
		MatrixElement[][] mapping = new MatrixElement[M][N];
		
		// Initialization
		for(int i = 0; i < M; i++) {		
			for(int j = 0; j < N; j++) {
				if(i == 0 && j == 0)
					mapping[i][j] = new MatrixElement(i, j, -1);
				else
					mapping[i][j] = new MatrixElement(i, j, 0);
			}
		}
		
		// Define row1 and col1 as the Partition IDs and HGraph Cluster IDs
		for(int i = 1; i < M; i++) {			
			mapping[i][0].setCounts(i);
			
			for(int j = 1; j < N; j++) {
				mapping[0][j].setCounts(j);
			}
		}
		
		int partition_id = -1;
		int cluster_id = -1;
		MatrixElement me;
				
		Set<Integer> dataSet = new TreeSet<Integer>();
		
		for(SimpleHEdge h : wb.hgr.getEdges()) {
			
			Transaction tr = wb.getTransaction(h.getId());
			
			for(Integer data_id : tr.getTr_dataSet()) {
				Data data = cluster.getData(data_id);
				
				if(!dataSet.contains(data.getData_id()) && data.isData_inUse()) {
					dataSet.add(data.getData_id());
					
					partition_id = data.getData_partition_id();
					
					switch(Global.workloadRepresentation) {
					
						case "hgr":
							if(Global.compressionEnabled)
								cluster_id = data.getData_chmetisClusterId();
							else
								cluster_id = data.getData_hmetisClusterId();
							
							break;
						
						case "gr":
							cluster_id = data.getData_metisClusterId();
							break;
					}					
															
					//System.out.println("@debug >> "+data.toString()+" | P"+partition_id+" | C"+cluster_id);										
					me = mapping[partition_id][cluster_id];
					//System.out.println("@debug >> Row = "+me.getRow_pos()+"| Col ="+me.getCol_pos());
					me.setCounts(me.getCounts()+1);
				}
			} // end -- for()-Data
		} // end -- for()

		// Create the Movement Matrix
		return (new Matrix(mapping));		 
	}
}