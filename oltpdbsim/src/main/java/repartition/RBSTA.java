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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import main.java.cluster.Cluster;
import main.java.cluster.Data;
import main.java.cluster.Partition;
import main.java.cluster.Server;
import main.java.entry.Global;
import main.java.utils.graph.SimpleHEdge;
import main.java.utils.graph.SimpleVertex;
import main.java.workload.Transaction;
import main.java.workload.WorkloadBatch;

/*
 * Repartition Based on Server-level Transactional Association (RBSTA)
 */

public class RBSTA {
	
	public static PriorityQueue<SimpleTr> pq;
	public static HashMap<Integer, SimpleTr> tMap;
	
	// Populates a priority queue to keep the potential transactions 
	public static void populatePQ(Cluster cluster, WorkloadBatch wb) {		
		if(Global.idt_priority >= Global.lb_priority)
			pq = new PriorityQueue<SimpleTr>(wb.hgr.getEdges().size(), SimpleTr.by_MAX_IDT_REDUCTION_IMPROVEMENT());
		else
			pq = new PriorityQueue<SimpleTr>(wb.hgr.getEdges().size(), SimpleTr.by_MAX_LB_IMPROVEMENT());		
		
		tMap = new HashMap<Integer, SimpleTr>();
				
		for(SimpleHEdge h : wb.hgr.getEdges()) {			
			SimpleTr t = prepare(cluster, wb, h);			
			tMap.put(t.id, t);
			
			if(!t.isProcessed)
				pq.add(tMap.get(t.id));
		}
	}
	
	// Prepares current transaction for processing
	private static SimpleTr prepare(Cluster cluster, WorkloadBatch wb, SimpleHEdge h) {
		Transaction tr = wb.getTransaction(h.getId());			
		SimpleTr t = new SimpleTr(tr.getTr_id(), tr.getTr_period());

		t.populateServerSet(cluster, tr);
				
		if(t.dataMap.size() > 1)	 // DTs
			t.populateMigrationList(cluster, wb);			
		else {						 // Movable non-DTs
			t.min_data_mgr = 0;
			t.max_idt_gain = 0.0;
			t.isProcessed = true;
		}
		
		return t;
	}
	
	// Checks whether processing current transaction affect any other transaction adversely
	public static boolean isAffected(WorkloadBatch wb, SimpleTr t, MigrationPlan m) {			
		// Search the incident transactions for the targeted data rows to be moved
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d);
				
				for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
					SimpleTr incidentT = tMap.get(h.getId());				
					
					if(!incidentT.equals(t) && incidentT.isProcessed) {					
						if(incidentT.dataMap.containsKey(m.to)) { // Either no change or potential reduction in the impact  
							return false;
						} else { // Destination server is not covered by the incident transaction						
							if(!isContainsAll(incidentT, m)) // Either no change or potential reduction in the impact
								return false;
							else // Not all of the data ids from the source servers are included in the migration plan
								return true;
						}
					}
				}
			}
		}
		
		return false;		
	}
	
	private static boolean isContainsAll(SimpleTr incidentT, MigrationPlan m) {
		
		boolean contains = false;
		
		for(int s_id : m.fromSet) {
			if(incidentT.dataMap.containsKey(s_id))
				if(incidentT.dataMap.get(s_id).containsAll(m.dataMap.get(s_id)))
					contains = true;
		}
		
		if(contains)
			return false;
		else
			return true;
	}
	
	public static void processTransaction(Cluster cluster, WorkloadBatch wb, SimpleTr t, MigrationPlan m) {
						
		// Data migrations
		HashMap<Integer, HashSet<Integer>> dataMap = new HashMap<Integer, HashSet<Integer>>(m.dataMap);			
		dataMigration(cluster, m.to, dataMap);
		
		// Adjust transaction's serverSet				
		for(int s_id : m.fromSet) {
			for(int d : t.dataMap.get(s_id))
				t.dataMap.get(m.to).add(d);			
			
			t.dataMap.remove(s_id);
		}
		
		// Update incident transactions
		for(Entry<Integer, HashSet<Integer>> entry : m.dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				SimpleVertex v = wb.hgr.getVertex(d);
				
				for(SimpleHEdge h : wb.hgr.getIncidentEdges(v)) {
					SimpleTr incidentT = tMap.get(h.getId());
					//System.out.println("\t\t--> "+incidentT.toString());
					
					if(!incidentT.equals(t) && !incidentT.isProcessed) {				
						pq.remove(incidentT);
						tMap.remove(h.getId());
						
						SimpleTr new_incidentT = prepare(cluster, wb, h);
						tMap.put(new_incidentT.id, new_incidentT);				
						
						 // Only DTs will be added back after recalculations
						if(new_incidentT.dataMap.size() > 1)				
							pq.add(tMap.get(new_incidentT.id));						
					}
				}
			}
		}
		
		t.isProcessed = true;
	} 
	
	// Perform data migrations
	private static void dataMigration(Cluster cluster, int dst_server_id, HashMap<Integer, HashSet<Integer>> dataMap) {
		// Chose the destination partition ids
		Server dst_server = cluster.getServer(dst_server_id);
		ArrayList<Partition> dst_partitionList = new ArrayList<Partition>();		
		
		for(int p : dst_server.getServer_partitions())
			dst_partitionList.add(cluster.getPartition(p));	

		Collections.sort(dst_partitionList, Partition.BY_DATA_SIZE());
		
		int dst_partition_id = 0;
		int r = 0;
		
		for(Entry<Integer, HashSet<Integer>> entry : dataMap.entrySet()) {
			for(int d : entry.getValue()) {
				Data data = cluster.getData(d);
		
				if(dataMap.size() > 1) {				
					if(r == dst_partitionList.size())
						r = 0;
					
					dst_partition_id = dst_partitionList.get(r).getPartition_id();				
					++r;
					
				} else {
					dst_partition_id = dst_partitionList.get(0).getPartition_id();
				}
				
				DataMigration.migrateSingleData(cluster, data, dst_server_id, dst_partition_id);		
			}
		}
	}
}