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

import java.util.Set;
import java.util.TreeSet;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.entry.Global;
import main.java.utils.Matrix;
import main.java.utils.MatrixElement;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.ISimpleHypergraph;
import main.java.utils.graph.SimpleVertex;
//import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

public class MappingTable {
	public MappingTable() {}
	
	public Matrix generateMappingTable(Cluster cluster, WorkloadBatch wb) {
		int M = 0;
		
		if(Global.associative)
			M = cluster.getServers().size()+1;
		else
			M = cluster.getPartitions().size()+1;
		
		int N = M; // Having a NxN matrix
		
		// Create a 2D Matrix to represent the Data movements due to partitioning decision
		MatrixElement[][] mapping = new MatrixElement[M][N];
		
		// Initialization
		int id = 0;
		for(int i = 0; i < M; i++) {		
			for(int j = 0; j < N; j++) {
				if(i == 0 && j == 0)
					mapping[i][j] = new MatrixElement(++id, i, j, -1);
				else
					mapping[i][j] = new MatrixElement(++id, i, j, 0);
			}
		}
		
		// Define row1 and col1 as the Partition IDs and HGraph Cluster IDs
		for(int i = 1; i < M; i++) {			
			mapping[i][0].setValue(i);
			
			for(int j = 1; j < N; j++) {
				mapping[0][j].setValue(j);
			}
		}
		
		if(Global.associative)
			processMapping(cluster, wb, Global.dsm.hgr, mapping);
		else
			processMapping(cluster, wb, wb.hgr, mapping);		

		// Create the Movement Matrix
		return (new Matrix(mapping));
	}
	
	private void processMapping(Cluster cluster, WorkloadBatch wb, 
			ISimpleHypergraph<SimpleVertex, SimpleHEdge> hgr, MatrixElement[][] mapping) {
		
		int partition_id = -1;
		int cluster_id = -1;
		MatrixElement me;				
		Set<Integer> dataSet = new TreeSet<Integer>();
		
		for(SimpleVertex v : hgr.getVertices()) {
			Data data = cluster.getData(v.getId());
			
			if(!dataSet.contains(data.getData_id()) && data.isData_inUse()) {
				dataSet.add(data.getData_id());
				
				if(Global.associative)
					partition_id = data.getData_server_id();
				else
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
														
				//Global.LOGGER.debug("@debug >> "+data.toString()+" | P"+partition_id+" | C"+cluster_id);										
				me = mapping[partition_id][cluster_id];
				//Global.LOGGER.debug("@debug >> Row = "+me.getRow_pos()+"| Col ="+me.getCol_pos());
				me.setValue(me.getValue()+1);
			}
		} // end -- for()-Data		
	}
}